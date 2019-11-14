# registry_sync
## Overview
Contains the following items:
* registry_sync.py - Syncs the contents of a remote registry to destination registry. Supports authentication.
* sample-config.yml - A sample configuration file for registry_sync.py

## More information on registry_sync.py
Python script which copies images from a remote registry to a destination registry using skopeo in the backend. Supports multi-threading for I/O so that images are copied faster and docker authentication with remote registries such as registry.redhat.io.

Any images it downloads to the registry will be tagged with both its current version-release as well as latest (or what is defined in destination_image_tag).
