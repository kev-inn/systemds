.. ------------------------------------------------------------------------------
..  Copyright 2020 Graz University of Technology
..
..  Licensed under the Apache License, Version 2.0 (the "License");
..  you may not use this file except in compliance with the License.
..  You may obtain a copy of the License at
..
..    http://www.apache.org/licenses/LICENSE-2.0
..
..  Unless required by applicable law or agreed to in writing, software
..  distributed under the License is distributed on an "AS IS" BASIS,
..  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
..  See the License for the specific language governing permissions and
..  limitations under the License.
.. ------------------------------------------------------------------------------

Install SystemDS
================

SystemDS can be installed via `pip`.

.. todo::

  Add instructions for building from source

Pip
---

Installation is quite simple with `pip`, just execute the following command::

  pip install systemds

SystemDS is a java-project, the `pip` package contains all the necessary `jars`,
but you will need java version 8 installed. Do not use an older or newer
version of java, because SystemDS is non compatible with other java versions.

Check the output of ``java -version``. Output should look similiar to::

  openjdk version "1.8.0_242"
  OpenJDK Runtime Environment (build 1.8.0_242-b08)
  OpenJDK 64-Bit Server VM (build 25.242-b08, mixed mode)

The important part is in the first line ``opendjdk version "1.8.0_xxx"``,
please make sure this is the case.
