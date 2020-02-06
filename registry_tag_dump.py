#!/usr/bin/env python
"""
Dumps the tagged version and SHA digest from a source registry for a list of container images.
"""
# Python 3 support
from __future__ import absolute_import
from __future__ import print_function
import six
from six.moves import range
import six.moves.queue

__author__ = "Phuong Nguyen (pnguyen@redhat.com)"
__copyright__ = "Copyright 2020"
__version__ = "20200206"
__maintainer__ = "Phuong Nguyen"
__email__ = "pnguyen@redhat.com"
__status__ = "Production"

import sys
import yaml
import json
import subprocess
import threading
from argparse import ArgumentParser

skopeo_cmd = "/bin/skopeo"
config = {}


class SkopeoWorker(threading.Thread):
    """
    Worker thread to run skopeo commands
    """

    def __init__(self, read_queue):
        threading.Thread.__init__(self)

        # used for setting/retrieving data for thread
        self.read_queue = read_queue

    def run(self):

        # make sure the thread doesn't die until the queue is empty
        while True:
            (container_image, config, tag) = self.read_queue.get()

            image_info = read_image(config["source_registry_credentials"]["user"],
                                    config["source_registry_credentials"]["token"],
                                    container_image["imagename"],
                                    config["source_tls_verify"], config["source_registry_type"], 
                                    tag)
            print("- imagename: {name}:{version}-{release}".format(name=image_info["Name"], 
                                                                    version=image_info["Labels"]["version"], 
                                                                    release=image_info["Labels"]["release"]))
            print("  digest: {digest}".format(digest=image_info["Digest"]))

            # will exit the infinite loop once the thread is finished
            self.read_queue.task_done()


def read_image(source_registry_user, source_registry_token, source_image, source_tls_verify, source_registry_type, tag):
    """
    Inspects an image from a remote registry using skopeo
    :param source_registry_user: Username used to authenticate against remote registry
    :param source_registry_token: Token used to authenticate against remote registry
    :param source_image: Source image on remote registry to inspect
    :param source_tls_verify: Required to be false if using http instead of https for remote registry
    :param source_registry_type: Usually docker://
    :param tag: Override default tag specified in container_images list
    :return: Dictionary
    """
    cmd_stdout = ""
    image_info = {}

    # In case the process call fails
    try:
        if not tag:
            cmd_stdout = subprocess.check_output([skopeo_cmd, "inspect", "--tls-verify=" + source_tls_verify, "--creds",
                                                source_registry_user + ":" + source_registry_token,
                                                source_registry_type + source_image])
        else:
            cmd_stdout = subprocess.check_output([skopeo_cmd, "inspect", "--tls-verify=" + source_tls_verify, "--creds",
                                                source_registry_user + ":" + source_registry_token,
                                                source_registry_type + source_image.rsplit(':', 1) + ":" + tag])

    except subprocess.CalledProcessError as e:
        print(("Error: Incorrect command {cmd}".format(cmd=str(e.cmd))))
        sys.exit(1)

    image_info = json.loads(cmd_stdout)

    return image_info


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
    parser.add_argument("-T", "--tag", help="Dump image info with specified tag",
                        default=False)

    return parser.parse_args()


def main():
    args = parse_args()
    config = load_config(args.config)
    read_queue = six.moves.queue.Queue()

    for container_image in config["container_images"]:
        read_queue.put((container_image, config, args.tag))

    # spawn new threads to do the registry read using skopeo
    for i in range(args.threads):
        worker = SkopeoWorker(read_queue)
        worker.daemon = True    # required otherwise the script will never exit
        worker.start()

    read_queue.join()


if __name__ == "__main__":
    main()
