

from distutils.core import setup

setup(name="gracc-xdmod",
      version="0.1",
      author="Mats Rynge",
      author_email="rynge@isi.edu",
      url="https://github.com/opensciencegrid/gracc-xdmod",
      description="Probe for synchronizing gracc and XDMoD",
      package_dir={"": "src"},
      packages=["gracc_xdmod"],

      scripts = ['src/gracc-xdmod'],

      #data_files=[("/etc/cron.d", ["config/gracc-xdmod.cron"]),
      #      ("/etc/", ["config/gracc-xdmod.cfg"]),
      #    ],

     )

