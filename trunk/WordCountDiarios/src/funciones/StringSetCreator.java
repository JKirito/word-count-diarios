package funciones;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class StringSetCreator {

	public StringSetCreator() {
	}

	public Set<String> getStringSet(File file) throws IOException {
		Set<String> stringSet = new HashSet<String>();
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);

		String linea = "";
		while ((linea = br.readLine()) != null) {
			stringSet.add(linea);
		}
		br.close();

		return stringSet;
	}

}
