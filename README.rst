apt_package_mirror
==================

A apt mirror tool that does staging to prevent breaking your mirror!

Requirements
~~~~~~~~~~~~

An ubuntu mirror takes up about 1T worth of data currently. This tool also
puts about 10G of data in /tmp so that it can stage the indices.

This tool uses rsync, please install it using your package manager.

Checks the tool makes
~~~~~~~~~~~~~~~~~~~~~

This tool checks the md5sum of each Package file and checks the Package indices
to make sure each package exists before moving the Package indices into place

How to use the tool
~~~~~~~~~~~~~~~~~~~

Check the example config file and change it to your needs, note the mirror you
mirror from must support rsync. Run the script using the following

Upgrading from version 1.X -> 2.Y
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Version 1.X and 2.Y use different formats for tracking old packages. For this
reason in order to update to version 2.Y you must delete
`$temp_files_path/files_to_delete`. 2.0 also introduces new configuration
options however they should always be backwards compatible, see
`example_config.yml` for examples and explanations of all the options.

.. code::

    python3 apt_package_mirror/__main__.py config.yaml

or install it using pip and run it using the following

.. code::

    pip install apt_package_mirror
    apt_package_mirror config.yaml

.. Note::

    As of version 2.0, python3 is supported, python2.7 may work but I do not
    intend to support it going forward.
