package at.mts.f2s;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

/**Files to SQLite (F2S). Converts all files in one directory into a sqlite database table where the files are blob entries and their names form the primary key or an optional column. */
public class F2S {
	/**Suffix for SQL column or variable generation.*/
	private final String SQL_SUFFIX = ", ";
	/**Contains all supported command line arguments. The array indices are organised as triplets. First one is a hyphen followed by one character denoting the command. Second one contains the description of the first index, the argument. Third index stores the argument parameter value if entered, otherwise the third index is false.*/
	private final String[] commands={
			/*00*/"-r","Remove the given regex (https://docs.oracle.com/javase/10/docs/api/java/util/regex/Pattern.html#sum) from the filename which must be an integer after regex replacement to serve as primary key.", null,
			/*03*/"-n","Name of the table to create. Default: file", "file",
			/*06*/"-h","Prints the help.", null,
			/*09*/"-i","Input absolute or relative path to directoy containing the files without any filenames and no slash at the end. Empty for current path.", null,
			/*12*/"-o","Output absolute or relative path with sqlite database filename to generate but without ending. Empty to generate f2s.sqlite on the current path.", null,
			/*15*/"-s","Prints the current status. Only considers all previous arguments.", null,
			/*18*/"-w","False, create table in databse without the rowid (https://sqlite.com/withoutrowid.html), true with rowid. Default is true.", "true",
			/*21*/"-d","True, execute drop table from -n before table creation, false do not. Default: true.", "true",
			/*24*/"-p","Zero or any positive number, start Primary Key (PK) from the specified number. Any negative number, use file names as PK, but they must be an positive integer after appyling -r. Default: -1", "-1",
			/*27*/"-1", "Primary key column name. Default: id.", "id",
			/*30*/"-2", "Filename column name. Only used when -p is zero or a positive integer. Default: filename.", "filename",
			/*33*/"-3", "Data (blob) column name. If the name is null, no blob entries are generated. Default: data.", "data",
			/*36*/"-f", "Find in the copied byte stream the given string and extract everything into an integer column where its name is denoted by -4. Data extraction is done until the string of -u is found which must be set too, otherwise -f and -u are ignored. Default: null.", null,
			/*39*/"-u", "Until, terminaing string for -f which must be set too, otherwise -f and -u are ignored. Default: null.", null,
			/*42*/"-4", "Column name of the found results inside he bye stream. Default: link.", "link",
			/*45*/"-e", "Replaces any characters in the filename with the given numerical argument to generate a number. If specified, he -2 column will be an integer. Default: null", null
	};

	/**Files to SQLite (F2S). Converts all files in one directory into a sqlite database table where the files are blob entries and their names form the primary key or an optional column.
	 * @param args of the tool. See {@link F2S#commands commands}. */
	public static void main(String[] args) {
		new F2S(args);
	}

	/**Files to SQLite (F2S). Converts all files in one directory into a sqlite database table where the files are blob entries and their names form the primary key or an optional column.
	 * @param args of the tool. See {@link F2S#commands commands}. */
	public F2S(String[] args) {
		for(int a = 0; a < args.length; a++) // Command line argument processing
			for(int c = 0; c+2 < commands.length; c+=3)
				if(commands[c].equals(args[a])) {
					if(commands[c].equals(commands[6])) {//-h, special case, no param setting just printing
						print(true);
						return;
					}
					else if(commands[c].equals(commands[15]))//-s
						print(false);
					else if(++a < args.length)
						commands[c+2] = args[a];
					break;
				}

		try {
			process();
		} catch (IOException | SQLException | URISyntaxException e) {
			e.printStackTrace();
		}
	}

	/**Converts the files with the given command line arguments.
	 * @throws IOException
	 * @throws SQLException
	 * @throws URISyntaxException */
	private void process() throws IOException, SQLException, URISyntaxException {
		String path = F2S.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath().replace("f2s.jar", "");

		if(commands[11] == null)//-i
			commands[11] = path;
		else if(commands[11].startsWith("./"))
			commands[11] = path + commands[11].substring(1, commands[11].length());
		else if(commands[11].startsWith("../"))
			commands[11] = path + commands[11];
		
		System.out.println("F2S: Database creation from\n " + commands[11] +"\n started.");

		if(commands[14] == null)//-o
			commands[14] = path + "/f2s.db";
		else if(commands[14].startsWith("./"))
			commands[14] = path + commands[14].substring(1, commands[14].length()) + ".db";
		else if(commands[14].startsWith("../"))
			commands[14] = path + commands[14] + ".db";

	
		commands[20] = Boolean.parseBoolean(commands[20]) ? "" : " WITHOUT ROWID";//-w

		if(commands[21] == null)//-d
			commands[21] = "true";

		if(commands[26] == null || Integer.parseInt(commands[26]) < 0)//-p
			commands[26] = "-1";
		
		if(commands[35].equalsIgnoreCase("null")) // -3, if "null" don't generate blob
			commands[35] = null;
		
		boolean nothing2Find = commands[38] == null || commands[41] == null;//-f || -u

		if(nothing2Find)//-f || -u
			commands[38] = commands[41] = commands[44] = null;

		boolean negativePK = commands[26].equals("-1");

		// DB creation
		Connection connection = DriverManager.getConnection("jdbc:sqlite:" + commands[14]);//-o
		Statement s = connection.createStatement();

		if(Boolean.getBoolean(commands[21]))//-d
			s.addBatch("DROP TABLE IF EXISTS " + commands[5]);

		createTableCreationNames(negativePK, nothing2Find);

		s.addBatch("CREATE TABLE IF NOT EXISTS " + commands[5] + commands[8] + commands[20]);

		for(int b : s.executeBatch())
			if(b == Statement.EXECUTE_FAILED)
				throw new SQLException("Deletion or creation of table failed.");

		s.clearBatch();
		s.close();

		createInsertTableNames(negativePK, nothing2Find);
		createInsertTableValues(negativePK, nothing2Find);

		int pk = 0;
		final boolean generateBlob = commands[35] != null; // -3, if blob generation is wanted
		final boolean fileNameIsText = commands[47] == null; // -e
		commands[5] = "INSERT INTO " + commands[5] + commands[8] + " VALUES " + commands[29]; // Insert statment executed for each single file found

		for (File f : new File(commands[11]).listFiles()) { //Slow but save and less memory consumption
			
			if(f.isDirectory()) {
				System.out.println("Subdirectoy " + f.getName() + "omitted.");
				continue;
			}
			
			FileInputStream fis = new FileInputStream(f);

			PreparedStatement ps = connection.prepareStatement(commands[5]);
			byte[] bytes = new byte[(int) f.length()]; // For fast reading, could be a  problem on bigger files, Integer.Maxvalue...
			String filename = f.getName();

			filename = filename.substring(0, filename.lastIndexOf(".")).replaceAll(commands[2], "");
			
			if(!fileNameIsText)
				filename = filename.replaceAll("[a-zA-Z]", commands[47]);
				
			fis.read(bytes, 0, bytes.length);
			fis.close();

			if(!nothing2Find) // Something is to find
				find(bytes); // result stored in commands[17]

			if(negativePK) { //-p = -1 use filename as PK, extra PK column otherwise
				ps.setInt(1, Integer.parseInt(filename));

				if(nothing2Find && generateBlob)// -3, if blob generation is wanted
					ps.setBytes(2, bytes);
				else { // Extraction in commands[17] denoted by -f and -u
					ps.setInt(2, Integer.parseInt(commands[17]));
					
					if(generateBlob) // -3, if blob generation is wanted
						ps.setBytes(3, bytes);
				}
			}
			else {
				ps.setInt(1, pk++);
				
				if(fileNameIsText) // -e, text column
					ps.setString(2, filename);
				else // -e, integer column
					ps.setInt(2, Integer.parseInt(filename));

				if(nothing2Find && generateBlob) // -3, if blob generation is wanted
					ps.setBytes(3, bytes);
				else { // Extraction in commands[17] denoted by -f and -u
					ps.setInt(3, Integer.parseInt(commands[17]));
					
					if(generateBlob) // -3, if blob generation is wanted
						ps.setBytes(4, bytes);
				}
			}

			ps.executeUpdate();
			ps.close();
		}

		connection.close();

		File f1 = new File(commands[14]);
		File f2 = new File(commands[14].replace(".db", ".sqlite"));

		f1.renameTo(f2);
		System.out.println("F2S: Database creation in\n " + commands[14] + "\n finished.");
	}

	/**
	 * Stores all table value place holders for an table insert in commands[29].
	 * @param negativePK is true if -p is < 0, false otherwise.
	 * @param nothing2Find is true if -f and -u are not null, false otherwise.
	 */
	private void createInsertTableValues(boolean negativePK, boolean nothing2Find) {
		commands[29] = "(?" + SQL_SUFFIX;
		String findValue = nothing2Find ? "" : "?" + SQL_SUFFIX; // -4, column value of the extracted value between -f and -u
		String blobValue = commands[35] == null ? "" : "?" + SQL_SUFFIX; // -3, if null no column
		
		if(negativePK)
			commands[29] += findValue + blobValue;
		else
			commands[29] += findValue + "?" + SQL_SUFFIX + blobValue;
		
		commands[29] = commands[29].substring(0, commands[29].lastIndexOf(SQL_SUFFIX)) + ")";
	}

	/**
	 * Stores all table names for an table insert in commands[8].
	 * @param negativePK is true if -p is < 0, false otherwise.
	 * @param nothing2Find is true if -f and -u are not null, false otherwise.
	 */
	private void createInsertTableNames(boolean negativePK, boolean nothing2Find) {
		String findColumn = nothing2Find ? "" : SQL_SUFFIX + commands[44]; // -4, column name of the extracted value between -f and -u
		String blobColumn = commands[35] == null ? "" : SQL_SUFFIX + commands[35]; // -3, if null no column

		if(negativePK)//-p = -1 use filename as PK, extra PK column otherwise
			commands[8] = "(" + commands[29] + findColumn + blobColumn + ")";
		else
			commands[8] = "(" + commands[29] + SQL_SUFFIX + commands[32] + findColumn + blobColumn + ")";
	}

	/**
	 * Stores all table names for table creation in commands[8].
	 * @param negativePK is true if -p is < 0, false otherwise.
	 * @param nothing2Find is true if -f and -u are not null, false otherwise.
	 */
	private void createTableCreationNames(boolean negativePK, boolean nothing2Find) {
		commands[8] = "(" + commands[29] + " INTEGER PRIMARY KEY NOT NULL" + SQL_SUFFIX;
		String columnType = commands[47] == null ? " TEXT " : " INTEGER "; 
				
		if(!negativePK)//-p > -1 use extra filename column
			commands[8] += commands[32] + columnType + "NOT NULL" + SQL_SUFFIX;

		if(!nothing2Find)// -f and -u are filled, use -4 as foreigen key column name
			commands[8] += commands[44] + " INTEGER NOT NULL" + SQL_SUFFIX;
		
		if(commands[35] == null)
			commands[8] = commands[8].substring(0, commands[8].lastIndexOf(SQL_SUFFIX)) + ")";
		else
			commands[8] += commands[35] + " BLOB NOT NULL)";
	}

	/** Searches in the argument the provided string of -f until the provided string of -u is found and stores the result in the parameter of -s, commands[17]. If nothing is found null is stored.
	 * @param bytes in which the search of parameter -f and -u is executed.
	 */
	private void find(byte[] toSearch) {
		byte[] startBytes = commands[38].getBytes(); // -f search string
		boolean equal = true;

		for(int b = 0; b < toSearch.length; b++) { // Go through all bytes to search
			equal = true;

			for(int s = 0; equal && s < startBytes.length && b+s < toSearch.length; s++)// Compare byte wise both byte ranges to find -f
				equal &= Byte.compare(toSearch[b+s], startBytes[s]) == 0;

			if(equal) { // Whole bite range is the same read into SB until -u is found
				commands[41] = commands[41].replace("\\n", "\n");
				byte[] endBytes = commands[41].getBytes(); // -u search string

				b = b + startBytes.length; // Save -f bytes end index = start of found term
				for(int a = b; a < toSearch.length; a++) {// Find -u after -f was found
					equal = true;

					for(int e = 0; equal && e < endBytes.length && a+e < toSearch.length; e++) // Compare byte wise both byte ranges to find -u
						equal &= Byte.compare(toSearch[a+e], endBytes[e]) == 0;

					if(equal) { // -u was found, convert everything from b, start of extraction, to a, end of extraction area denoted by -f and -u
						commands[17] = new String(Arrays.copyOfRange(toSearch, b, a), StandardCharsets.UTF_8);
						break;
					}
				}

				break;
			}
		}
	}

	/**Prints the help or internal status.*/
	private void print(boolean help) {
		StringBuilder sb = new StringBuilder((help?"Help":"Status")+ " of " + this.getClass().getSimpleName() + ":\n");

		for(int i = 0; i+2 < commands.length;i+=3)
			sb.append(String.format("  %2s\n    %s\n\n", commands[i], commands[i+(help?1:2)]));

		System.out.println(sb.toString());
		sb.setLength(0);
	}
}
