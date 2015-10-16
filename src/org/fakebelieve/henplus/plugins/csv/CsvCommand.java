/*
 * This is free software, licensed under the Gnu Public License (GPL)
 * get a copy from <http://www.gnu.org/licenses/gpl.html>
 */
package org.fakebelieve.henplus.plugins.csv;

import henplus.AbstractCommand;
import henplus.CommandDispatcher;
import henplus.HenPlus;
import henplus.SQLSession;
import henplus.commands.TimeRenderer;
import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public final class CsvCommand extends AbstractCommand {

    private static final String COMMAND_SAVE_FILE = "csv";

    /**
     *
     */
    public CsvCommand() {
    }

    /*
     * (non-Javadoc)
     * @see henplus.Command#getCommandList()
     */
    @Override
    public String[] getCommandList() {
        return new String[]{COMMAND_SAVE_FILE};
    }

    /*
     * (non-Javadoc)
     * @see henplus.Command#participateInCommandCompletion()
     */
    @Override
    public boolean participateInCommandCompletion() {
        return true;
    }

    protected List<String> getCatalogs(SQLSession session) throws SQLException {
        List<String> list = new ArrayList<String>();
        ResultSet catalogs = session.getConnection().getMetaData().getCatalogs();
        while (catalogs.next()) {
            list.add(catalogs.getString(1));
        }
        catalogs.close();

        return list;
    }

    /*
     * (non-Javadoc)
     * @see henplus.Command#execute(henplus.SQLSession, java.lang.String, java.lang.String)
     */

    @Override
    public int execute(SQLSession session, String command, String parameters) {
        int result = SUCCESS;

        // required: session
        if (session == null) {
            HenPlus.msg().println("You need a valid session for this command.");
            return EXEC_FAILED;
        }

        if (command.equals(COMMAND_SAVE_FILE) ) {
            final StringTokenizer st = new StringTokenizer(parameters);
            final int argc = st.countTokens();

            if (argc < 2) {
                return SYNTAX_ERROR;
            }

            String fileName = st.nextToken().trim();
            String selectSql = st.nextToken("").trim();

            session.println("Saving to fileName " + fileName);

            Statement statement = session.createStatement();
            ResultSet resultSet = null;

            try {
                try {
                    resultSet = statement.executeQuery(selectSql);
                } catch (SQLException e) {
                    HenPlus.msg().println(e.getMessage());
                    return EXEC_FAILED;
                }

                ResultSetMetaData metaData = null;
                int columns;
                String[] columnNames;
                try {
                    metaData = resultSet.getMetaData();
                    columns = metaData.getColumnCount();
                    columnNames = new String[columns];
                    for (int idx = 0; idx < columns; idx++) {
                        columnNames[idx] = metaData.getColumnLabel(idx + 1);
                    }
                } catch (SQLException e) {
                    HenPlus.msg().println(e.getMessage());
                    return EXEC_FAILED;
                }


                try {
                    final long startTime = System.currentTimeMillis();
                    long lapTime = -1;
                    long execTime = -1;

                    BufferedWriter out = new BufferedWriter(new FileWriter(fileName, false));
                    ICsvListWriter listWriter = new CsvListWriter(out, CsvPreference.STANDARD_PREFERENCE);

                    listWriter.writeHeader(columnNames);

                    int rows = 0;

                    for (int count = 1; resultSet.next(); count++, rows++) {
                        List<Object> values = new LinkedList<Object>();
                        for (int idx = 0; idx < columns; idx++) {
                            values.add(resultSet.getObject(idx + 1));
                        }
                        listWriter.write(values);

                        lapTime = System.currentTimeMillis() - startTime;
                    }

                    listWriter.close();
                    out.close();

                    session.println(rows + " row" + (rows == 1 ? "" : "s") + " in result");

                    execTime = System.currentTimeMillis() - startTime;
                    session.print(" (");
                    if (lapTime > 0) {
                        session.print("first row: ");
                        if (session.printMessages()) {
                            TimeRenderer.printTime(lapTime, HenPlus.msg());
                        }
                        session.print("; total: ");
                    }
                    if (session.printMessages()) {
                        TimeRenderer.printTime(execTime, HenPlus.msg());
                    }
                    session.println(")");

                } catch (SQLException e) {
                    HenPlus.msg().println(e.getMessage());
                    return EXEC_FAILED;
                } catch (IOException e) {
                    HenPlus.msg().println(e.getMessage());
                    return EXEC_FAILED;
                }
            }
            finally {
                if (resultSet != null) {
                    try {
                        resultSet.close();
                    } catch (SQLException e) {
                        // IGNORE
                    }
                }
                if (statement != null) {
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        // IGNORE
                    }
                }
            }
        }

        return result;
    }

    /*
     * (non-Javadoc)
     * @see henplus.Command#requiresValidSession(java.lang.String)
     */
    @Override
    public boolean requiresValidSession(String cmd) {
        return true;
    }

    /*
     * (non-Javadoc)
     * @see henplus.Command#shutdown()
     */
    @Override
    public void shutdown() {
    }

    /*
     * (non-Javadoc)
     * @see henplus.Command#getShortDescription()
     */
    @Override
    public String getShortDescription() {
        return "save results to csv file";
    }

    /*
     * (non-Javadoc)
     * @see henplus.Command#getSynopsis(java.lang.String)
     */
    @Override
    public String getSynopsis(String cmd) {
        if (cmd.equals(COMMAND_SAVE_FILE)) {
            return cmd + " <csv-file> select ...";
        }
        return cmd;
    }

    /*
     * (non-Javadoc)
     * @see henplus.Command#getLongDescription(java.lang.String)
     */
    @Override
    public String getLongDescription(String cmd) {
        if (cmd.equals(COMMAND_SAVE_FILE)) {
            return "\tSave the output of a SELECT as CSV.\n"
                    + "\n"
                    + "\t\t" + COMMAND_SAVE_FILE + " <csv-file> select ...;\n"
                    + "\n";
        }
        return null;
    }

    @Override
    public Iterator complete(CommandDispatcher disp, String partialCommand, String lastWord) {
        HenPlus.getInstance().getCurrentSession();

        try {
            List<String> catalogs = getCatalogs(HenPlus.getInstance().getCurrentSession());
            for (Iterator<String> i = catalogs.listIterator(); i.hasNext(); ) {
                String catalog = i.next();
                if (!catalog.startsWith(lastWord)) {
                    i.remove();
                }
            }
            return catalogs.iterator();
        } catch (SQLException ex) {
            HenPlus.msg().println("Problem - " + ex.getMessage());
            return super.complete(disp, partialCommand, lastWord);
        }

    }

    /**
     * looks, if this word is contained in 'all', preceeded and followed by a whitespace.
     */
    private boolean containsWord(final String all, final String word) {
        final int wordLen = word.length();
        final int index = all.indexOf(word);
        return index >= 0 && (index == 0 || Character.isWhitespace(all.charAt(index - 1)))
                && Character.isWhitespace(all.charAt(index + wordLen));
    }

    public boolean isComplete(String command) {
        command = command.toUpperCase(); // fixme: expensive.
        if (command.startsWith("COMMIT") || command.startsWith("ROLLBACK")) {
            return true;
        }
        // FIXME: this is a very dumb 'parser'.
        // i.e. string literals are not considered.
        final boolean anyProcedure = command.startsWith("BEGIN")
                || command.startsWith("DECLARE")
                || (command.startsWith("CREATE") || command.startsWith("REPLACE"))
                && (containsWord(command, "PROCEDURE") || containsWord(command, "FUNCTION") || containsWord(command, "PACKAGE") || containsWord(
                command, "TRIGGER"));

        if (!anyProcedure && command.endsWith(";")) {
            return true;
        }
        // sqlplus is complete on a single '/' on a line.
        if (command.length() >= 3) {
            final int lastPos = command.length() - 1;
            if (command.charAt(lastPos) == '\n' && command.charAt(lastPos - 1) == '/' && command.charAt(lastPos - 2) == '\n') {
                return true;
            }
        }
        return false;
    }

}
