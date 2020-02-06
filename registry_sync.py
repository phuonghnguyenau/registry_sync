#!/usr/bin/env python
"""
Syncs a list of container images (as defined in the configuration file) from a remote registry to destination registry.
"""
# Python 3 support
from __future__ import absolute_import
from __future__ import print_function
from six.moves import range
import six.moves.queue

__author__ = "Phuong Nguyen (pnguyen@redhat.com)"
__copyright__ = "Copyright 2019, 2020"
__version__ = "20200206"
__maintainer__ = "Phuong Nguyen"
__email__ = "pnguyen@redhat.com"
__status__ = "Production"

import sys
import yaml
import json
import subprocess
import threading
import time
from argparse import ArgumentParser

skopeo_cmd = "/bin/skopeo"
config = {}


class SkopeoWorker(threading.Thread):
    """
    Worker thread to run skopeo commands
    """

    def __init__(self, sync_queue):
        threading.Thread.__init__(self)

        # used for setting/retrieving data for thread
        self.sync_queue = sync_queue

    def run(self):
        while True:
            (container_image, config, no_modify) = self.sync_queue.get()

            image_info = read_image(config["source_registry_credentials"]["user"],
                                    config["source_registry_credentials"]["token"],
                                    container_image["imagename"],
                                    config["source_tls_verify"], config["source_registry_type"])
            print(("Read container image: {name}".format(name=container_image["imagename"])))

            # try not to overwhelm registry server
            time.sleep(5)

            if not no_modify:
                copy_image(config["source_registry_credentials"]["user"],
                        config["source_registry_credentials"]["token"],
                        image_info, config["source_tls_verify"],
                        config["source_registry_type"],
                        config["destination_tls_verify"],
                        config["destination_registry_type"],
                        config["destination_registry_namespace"],
                        config["destination_image_tag"])
                print(("Copied container image: {name} to {namespace}".format(name=container_image["imagename"],
                                                                            namespace=config["destination_registry_namespace"])))
            # try not to overwhelm registry server
            time.sleep(5)

            # will exit the infinite loop once the thread is finished
            self.sync_queue.task_done()


def read_image(source_registry_user, source_registry_token, source_image, source_tls_verify, source_registry_type):
    """
    Inspects an image from a remote registry using skopeo
    :param source_registry_user: Username used to authenticate against remote registry
    :param source_registry_token: Token used to authenticate against remote registry
    :param source_image: Source image on remote registry to inspect
    :param source_tls_verify: Required to be false if using http instead of https for remote registry
    :param source_registry_type: Usually docker://
    :return: Dictionary
    """
    cmd_stdout = ""
    image_info = {}

    # In case the process call fails
    try:
        cmd_stdout = subprocess.check_output([skopeo_cmd, "inspect", "--tls-verify=" + source_tls_verify, "--creds",
                                            source_registry_user + ":" + source_registry_token,
                                            source_registry_type + source_image])

    except subprocess.CalledProcessError as e:
        print(("Error: Incorrect command {cmd}".format(cmd=str(e.cmd))))
        sys.exit(1)

    image_info = json.loads(cmd_stdout)

    return image_info

def copy_image(source_registry_user, source_registry_token, image_info, source_tls_verify, source_registry_type,
               destination_tls_verify, destination_registry_type, destination_registry_namespace,
               destination_image_tag):
    """
    Copies an image from a remote registry to destination registry using skopeo
    :param source_registry_user: Username used to authenticate against remote registry
    :param source_registry_token: Token used to authenticate against remote registry
    :param image_info: Dictionary containing image details
    :param source_tls_verify: Required to be false if using http instead of https for remote registry
    :param source_registry_type: Usually docker://
    :param destination_tls_verify: Required to be false if using http instead of https for destination registry
    :param destination_registry_type: Usually docker://
    :param destination_registry_namespace: Container registry namespace
    :param destination_image_tag: The additional tag to use for the image copied to destination
    :return: None
    """

    try:
        # Copy image tagged version-release to registry
        subprocess.check_output([skopeo_cmd, "copy", "--src-tls-verify=" + source_tls_verify, "--src-creds",
                                 source_registry_user + ":" + source_registry_token, "--dest-tls-verify=" +
                                 destination_tls_verify, source_registry_type + image_info["Name"] + ":" +
                                 image_info["Labels"]["version"] + "-" + image_info["Labels"]["release"],
                                 destination_registry_type + destination_registry_namespace +
                                 image_info["Labels"]["name"] + ":" + image_info["Labels"]["version"] +
                                 "-" + image_info["Labels"]["release"]])

        # Copy image tagged latest to registry
        # If image layers already exist, copy just recreates the tag
        # For example: registry.gpslab.cbr.redhat.com/rhosp13/openstack-aodh-evaluator:latest
        subprocess.check_output([skopeo_cmd, "copy", "--src-tls-verify=" + source_tls_verify, "--src-creds",
                                 source_registry_user + ":" + source_registry_token, "--dest-tls-verify=" +
                                 destination_tls_verify, source_registry_type + image_info["Name"] + ":" +
                                 image_info["Labels"]["version"] + "-" + image_info["Labels"]["release"],
                                 destination_registry_type + destination_registry_namespace +
                                 image_info["Labels"]["name"] + ":" + destination_image_tag])

    # usually if the wrong arguments have been passed to skopeo due to incorrect configuration file
    except subprocess.CalledProcessError as e:
        print(("Error: Incorrect command {cmd}".format(cmd=str(e.cmd))))
        sys.exit(1)


def load_config(config_file):
    """
    Loads a YAML configuration file into a Dictionary
    :param config_file: Filename of the configuration file
    :return: Dictionary
    """
    try:
        if six.PY2:
            config = yaml.load(open(config_file))
        else:
            config = yaml.load(open(config_file), Loader=yaml.FullLoader)

    except IOError as e:
        print(("Error: {msg}".format(msg=e.message)))
        sys.exit(1)

    except (yaml.scanner.ScannerError, yaml.parser.ParserError):
        print(("Error: Invalid YAML for config file {c}".format(c=config_file)))
        sys.exit(1)

    return config


def parse_args():
    """
    Adds command line arguments to script
    :return: argparse.Namespace object
    """
    parser = ArgumentParser()

    parser.add_argument("-c", "--config", help="Configuration file",
                        default="sample-config.yml")
    parser.add_argument("-t", "--threads", help="Number of threads to use",
                        default=5, type=int)
    parser.add_argument("-N", "--no-modify", help="Do not modify destination registry.",
                        action="store_true")

    return parser.parse_args()


def main():
    args = parse_args()
    config = load_config(args.config)
    sync_queue = six.moves.queue.Queue()

    for container_image in config["container_images"]:
        sync_queue.put((container_image, config, args.no_modify))
        
    # spawn new threads to do the registry sync using skopeo
    print("***** Initiating registry sync... *****")
    for i in range(args.threads):
        worker = SkopeoWorker(sync_queue)
        worker.daemon = True    # required otherwise the script will never exit
        worker.start()

    # process all threads until completed
    sync_queue.join()

    print("Registry sync completed!")


if __name__ == "__main__":
    main()
