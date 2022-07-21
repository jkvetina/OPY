# Patch structure

## Folders

This is the proposed folder structure:
    - 10_init
    - 20_diffs---MANUALLY
    - 30
    - 40_objects---LIVE
    - 60_cleanup
    - 70_data
    - 80_finally
    - 90_apex_app---LIVE

All files and folders are sorted alphabetically before executing.
You can rename, add/remove any file or folder.

Files/folders with flag ---LIVE are ignored in Git because they are just compiled/copyied from already versioned sources.
Files/folders with flag ---SKIP are skipped in patch creation.
Flag ---MANUALLY is just a notification to you where you should put the human made changes (ALTER statements and related data changes).

### Folder 10_init

You can put all kind of scripts here, which will be run before other scripts.
For example setting up a session parametres.

### Folder 20_diffs---MANUALLY

When you run export with -patch then the file for current date will be created in this folder.
You should add all ALTER statements and data changes here.

### Folder 30

in folder 20 and before recreating objects in folder 40.
For example you can drop all objects before recreating them (not that you should).
I sometimes do that to get rid of obsolete/depreciated objects
and to make sure I dont have extra/unwanted grants on them.

### Folder 40_objects---LIVE

In this folder you will see objects changed since last rollout.
No need to version this folder, because you already version the individual objects.

### Folder 60_cleanup

In this folder you have the opportunity to do some check and cleanup after all the objects were recreated.
Typically I recompile invalid objects, refresh stats if needed, move indexes to proper tablespace, recreate grants or ACLs...

### Folder 70_data

Some data you would like to create after all new objects are present,
or you would like to do some data changes on every release (like recreate LOVs), so you can do that here.

### Folder 80_finally

You can do some checks here.

### Folder 90_apex_app---LIVE

This folder contain copied APEX application(s). No need to version this folder.
If your app is missing, you will put it here and then it will be automatically refreshed.

