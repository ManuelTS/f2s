import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**Files to SQLite (F2S). Converts all files in one directory into a sqlite database table where the files are blob entries and their names form the primary key or an optional column. */
public class F2S {
	/**Contains all supported command line arguments. The array indices are organised as triplets. First one is a hyphen followed by one character denoting the command. Second one contains the description of the first index, the argument. Third index stores the argument parameter value if entered, otherwise the third index is false.*/
	private final String[] commands={
			"-r","Remove the given regex (https://docs.oracle.com/javase/10/docs/api/java/util/regex/Pattern.html#sum) from the filename which must be an integer after regex replacement to serve as primary key.", null,
			"-n","Name of the table to create. Default: file", "file",
			"-h","Prints the help.", null,
			"-i","Input absolute or relative path to directoy containing the files without any filenames and no slash at the end. Empty for current path.", null,
			"-o","Output absolute or relative path with sqlite database filename to generate but without ending. Empty to generate f2s.sqlite on the current path.", null,
			"-s","Prints the current status. Only considers all previous arguments.", null,
			"-w","False, create table in databse without the rowid (https://sqlite.com/withoutrowid.html), true with rowid. Default is true.", "true",
			"-d","True, execute drop table from -n before table creation, false do not. Default: true.", "true",
			"-p","Zero or any positive number, start Primary Key (PK) from the specified number. Any negative number, use file names as PK, but they must be an positive integer after appyling -r. Default: -1", "-1",
			"-1", "Primary key column name. Default: id.", "id",
			"-2", "Filename column name. Only used when -p is zero or a positive integer. Default: filename.", "filename",
			"-3", "Data (blob) column name. Default: data.", "data"
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
		System.out.println("F2S: Database creation started.");
		String path = F2S.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath().replace("/f2s.jar", "");

		if(commands[11] == null)//-i
			commands[11] = path;
		else if(commands[11].startsWith("./"))
			commands[11] = path + commands[11].substring(1, commands[11].length());

		if(commands[14] == null)//-o
			commands[14] = path + "/f2s.db";
		else if(commands[14].startsWith("./"))
			commands[14] = path + commands[14].substring(1, commands[14].length()) + ".db";

		if(commands[18] == null || !Boolean.getBoolean(commands[18]))//-w
			commands[18] = " WITHOUT ROWID";
		else
			commands[18] = "";

		if(commands[21] == null)//-d
			commands[21] = "true";

		if(commands[26] == null || Integer.parseInt(commands[26]) < 0)//-p
			commands[26] = "-1";

		boolean negativePK = commands[26].equals("-1");

		// DB creation
		Connection connection = DriverManager.getConnection("jdbc:sqlite:" + commands[14]);//-o
		Statement s = connection.createStatement();

		if(Boolean.getBoolean(commands[21]))//-d
			s.addBatch("DROP TABLE IF EXISTS " + commands[5]);

		if(negativePK)//-p = -1 use filename as PK, extra PK column otherwise
			commands[8] = "(" + commands[29] + " INTEGER PRIMARY KEY NOT NULL, ";
		else
			commands[8] = "(" + commands[29] + " INTEGER PRIMARY KEY NOT NULL, " + commands[32] + " TEXT NOT NULL, ";

		s.addBatch("CREATE TABLE IF NOT EXISTS " + commands[5] + commands[8] + commands[35] + " BLOB NOT NULL)" + commands[18]);

		for(int b : s.executeBatch())
			if(b == Statement.EXECUTE_FAILED)
				throw new SQLException("Deletion or creation of table failed.");

		s.clearBatch();
		s.close();

		if(negativePK)//-p = -1 use filename as PK, extra PK column otherwise
			commands[8] = "(" + commands[29] + ",  " + commands[35] + ")";
		else
			commands[8] = "(" + commands[29] + ", " + commands[32] + ",  " + commands[35] + ")";

		int pk = 0;

		for (File f : new File(commands[11]).listFiles()) { //Slow but save and less memory consumption
			FileInputStream fis = new FileInputStream(f);

			if(f.isDirectory()) {
				System.out.println("Subdirectoy " + f.getName() + "omitted");
				continue;
			}

			PreparedStatement ps = connection.prepareStatement("INSERT INTO " + commands[5] + commands[8] + " VALUES (?, " + (negativePK ? "?" : "?, ?" + ")"));
			byte[] bytes = new byte[(int) f.length()];
			String filename = f.getName();

			filename = filename.substring(0, filename.lastIndexOf("."));
			fis.read(bytes, 0, bytes.length);
			fis.close();

			if(negativePK) { //-p = -1 use filename as PK, extra PK column otherwise
				ps.setInt(1, Integer.parseInt(filename.replaceAll(commands[2], "")));
				ps.setBytes(2, bytes);
			}
			else {
				ps.setInt(1, pk++);
				ps.setString(2, filename.replaceAll(commands[2], ""));
				ps.setBytes(3, bytes);
			}

			ps.executeUpdate();
			ps.close();
		}

		connection.close();

		File f1 = new File(commands[14]);
		File f2 = new File(commands[14].replace(".db", ".sqlite"));

		f1.renameTo(f2);
		System.out.println("F2S: Database creation finished.");
	}

	/**Prints the help or internal status.*/
	private void print(boolean help) {
		StringBuilder sb = new StringBuilder((help?"Help":"Status")+ " of " + this.getClass().getSimpleName() + ":\n");

		for(int i = 0; i+2 < commands.length;i+=3)
			sb.append(String.format("%2s\n  %s\n\n", commands[i], commands[i+(help?1:2)]));

		System.out.println(sb.toString());
		sb.setLength(0);
	}
}
