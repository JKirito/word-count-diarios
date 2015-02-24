package wordCountJavi;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import funciones.StringSetCreator;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Set<String> stopWordsSet;
	private Set<String> stopSpecialWordsSet;
	private Set<String> caseSensitiveWordsSet;

	protected void setup(Context context) throws IOException {
		File stopWordsFile = new File("stopWords/stopWordsList.txt");
		stopWordsSet = new HashSet<String>();

		File stopSpecialWordsFile = new File("stopWords/stopSpecialWordsList.txt");
		stopSpecialWordsSet = new HashSet<String>();

		File caseSensitiveWordsFile = new File("stopWords/caseSensitiveWordsList.txt");
		caseSensitiveWordsSet = new HashSet<String>();

		StringSetCreator set = new StringSetCreator();

		stopWordsSet = set.getStringSet(stopWordsFile);
		stopSpecialWordsSet = set.getStringSet(stopSpecialWordsFile);
		caseSensitiveWordsSet = set.getStringSet(caseSensitiveWordsFile);
		System.out.println(stopWordsSet.size());
		System.out.println(stopSpecialWordsSet.size());
		System.out.println(caseSensitiveWordsSet.size());
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] words = value.toString().replaceAll("[[0-9]+/*\\.*[0-9]*|«|»|\"|?|¿|!|¡|\\[|\\]|)|(|'|#|,|%|&|;|:|0-9]", " ").split(" ");

		for (int i = 0; i < words.length; i++) {
			String cleanWord = limpiarPalabra(words[i], stopSpecialWordsSet, caseSensitiveWordsSet, stopWordsSet);
			if (!cleanWord.equals("")) {
				context.write(new Text(cleanWord), new IntWritable(1));
			}
		}
	}

	private String limpiarPalabra(String palabra, Set<String> stopSpecialWordsSet, Set<String> caseSensitiveWordsSet,
			Set<String> stopWordsSet) {
		palabra = palabra.replaceAll(" ", "");
		if (palabra.trim().isEmpty() || palabra.length() <= 1) {
			return "";
		}
		// si primer o último caracter es "raro", lo saco.
		// Puede haber varios caracteres seguidos "raros"
		int caracterFinal = palabra.length();
		for (int i = 0; i < caracterFinal; i++) {
			String primerCaracter = palabra.charAt(i) + "";
			if (stopSpecialWordsSet.contains(primerCaracter)) {
				primerCaracter = acomodarCaracterEspecial(primerCaracter);
				palabra = palabra.replace(primerCaracter, "");
				i = 0;
				caracterFinal = palabra.length();
			} else {
				break;
			}
		}
		if (palabra.length() <= 1) {
			return "";
		}
		for (int i = palabra.length() - 1; i >= 0; i--) {
			String ultimoCaracter = palabra.charAt(i) + "";
			if (stopSpecialWordsSet.contains(ultimoCaracter)) {
				ultimoCaracter = acomodarCaracterEspecial(ultimoCaracter);
				palabra = palabra.replace(ultimoCaracter, "");
				i = palabra.length() - 1;
			} else {
				break;
			}
		}
		if (!caseSensitiveWordsSet.contains(palabra)) {
			palabra = palabra.toLowerCase();
		}
		if (stopWordsSet.contains(palabra) || stopSpecialWordsSet.contains(palabra)) {
			return "";
		}
		return palabra;
	}

	private String acomodarCaracterEspecial(String palabra) {
		if (palabra.equals("\\")) {
			palabra += "\\";
		}
		return palabra;
	}
}