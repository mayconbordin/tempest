# Tempest

Tempest is a distributed stream processing system written in node.js.

## Installation

1. Clone the repository (or download the ZIP) in each node that will compose the cluster. The installation directory should be the same for all nodes.
2. Edit the file `bin/tempest` and change the variable `TEMPEST_HOME` to the full path to the directory where the system was installed. Also change `NODEJS_BIN` to point to the node.js binary.
3. Edit the `config/default.js` file and set the location of the master and the slaves.
4. Make sure that all nodes have passwordless ssh access to each other.
5. That's it.

## Command Line

- **start**
- **stop**
- **status**
- **daemon**
- **master**
- **app submit <app_file>**
- **app start**
- **app stop**
- **app graph**
- **app plan**

## License

The MIT License (MIT)
Copyright (c) 2013 Maycon Bordin

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
