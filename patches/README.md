# Patch structure

All folders and then files are sorted alphabetically before executing.
You can rename, add/remove any file or folder.

All these folders represents the structure/template around your database changes.



### Folder 10_init

You can put all kind of scripts here, which will be run before other scripts.
For example setting up a session parametres.

### Folder 30_table+data_changes

This is the last folder executed before your table changes.
When you run export with -patch then the file for current date will be created in this folder.
You should add all ALTER statements and data changes here.

### Folder 40

This is the last folder executed before your object changes.
For example you can drop all objects before recreating them (not that you should).
I sometimes do that to get rid of obsolete/depreciated objects
and to make sure I dont have extra/unwanted grants on them.

### Folder 90

In this folder you have the opportunity to do some check and cleanup after all the objects were recreated.
Typically I recompile invalid objects, refresh stats if needed, move indexes to proper tablespace, recreate grants or ACLs...

