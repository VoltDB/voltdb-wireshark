# voltdb-wireshark

This is a Wireshark plugin (packet dissector) for the VoltDB client wire protocol. The VoltDB client wire protocol is used by VoltDB servers and clients to communicate with each other. This plugin will help you understand the content in the packets using this protocol.

To learn more about the protocol, see the document [here](http://downloads.voltdb.com/documentation/wireprotocol.pdf).

## Get Started
* This plugin requires Wireshark **1.12.5** or later.
* Check Lua availability for your Wireshark. Lua has shipped with the Windows version of Wireshark since 0.99.4. Availability on other platforms varies. To see if your version of Wireshark supports Lua, go to Help→About Wireshark and look for Lua in the "Compiled with" paragraph.

  ![alt text](https://wiki.wireshark.org/Lua?action=AttachFile&do=get&target=lua-about.png "Lua availability")  
  *(The image is from wireshark wiki)*

* Find the file `init.lua` under configuration directory where the Wireshark Lua configuration is stored. On Windows you usually can find this file under `C:\Program Files\Wireshark`. On Mac OS X, this path normally is `/Applications/Wireshark.app/Contents/Resources/share/wireshark`
* Add one line to the end of `init.lua`:
```lua
dofile(DATA_DIR.."voltcw.lua")
```
* Copy `voltcw.lua` to the configuration directory.
