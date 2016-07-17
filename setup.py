import setuptools

setuptools.setup(
        install_requires=['pyyaml'],
        author = 'Caleb Boylan',
        name = 'apt-package-mirror',
        description = 'Python script for running an apt package mirror',
        author_email = 'calebboylan@gmail.com',
        url = 'https://github.com/squidboylan/apt-package-mirror',
        version = '0.1.1',
        classifiers = [
            'Development Status :: 3 - Alpha',
            'Intended Audience :: System Administrators',
            'License :: OSI Approved :: Apache Software License',
            'Operating System :: POSIX :: Linux',
            'Programming Language :: Python',
            'Programming Language :: Python :: 2',
            'Programming Language :: Python :: 2.7',
        ],
        packages=setuptools.find_packages(),
        entry_points = {
            'console_scripts': ['apt-package-mirror=mirror:main'],
        }
)
