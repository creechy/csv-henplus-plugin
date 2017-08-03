## CSV Henplus Plug-In ##

This plugin allows you to save the results of a SELECT statement to a file as CSV. 


### Easy Setup ###

Simply put `csv-henplus-plugin.jar` and `super-csv-2.3.1.jar` in to the CLASSPATH of `henplus`, generally in the `share/henplus` folder somewhere.

Start `henplus` and register the plugin. Use the `plug-in` command for this. This only needs to be done once, and will be persisted.

     Hen*Plus> plug-in org.fakebelieve.henplus.plugins.csv.CsvCommand

### Usage ###

The plugin responds to one command `csv`.

