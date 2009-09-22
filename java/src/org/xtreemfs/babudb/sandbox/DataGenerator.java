package org.xtreemfs.babudb.sandbox;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.Map.Entry;

public class DataGenerator {

	public static final Iterator fileIterator(final ArrayList<byte[]> lookupHits, final String filename) {
		char[] bytes = null;
		
		try {
			File f = new File(filename);
			FileReader fr = new FileReader(filename);
			bytes = new char[(int) f.length()];

			//while(fr.)
			int index = 0;
			for(int charsRead = 0; charsRead > -1; index += 1)
				charsRead = fr.read(bytes, index, bytes.length - index);
			fr.close();
			
		} catch(IOException e) {
			// TODO: handle error
		}
		
		final ArrayList<String> lines = new ArrayList<String>();

		int index = 0;
		while(index < bytes.length) {
			StringBuffer sb = new StringBuffer();
			while(bytes[index] != '\n') {
				sb.append(bytes[index]);
			}
			lines.add(sb.toString());
		}
		
		final Iterator<String> it = lines.iterator();
		
		return new Iterator<Entry<byte[], byte[]>>() {
			@Override
			public boolean hasNext() {
				return it.hasNext();
			}

			@Override
			public Entry<byte[], byte[]> next() {
				return new Entry<byte[], byte[]>() {

					final byte[] nextBytes = it.next().getBytes();

					@Override
					public byte[] getKey() {
						return nextBytes;
					}

					@Override
					public byte[] getValue() {
						return nextBytes;
					}

					@Override
					public byte[] setValue(byte[] value) {
						throw new UnsupportedOperationException();
					}
				};
			}

			@Override
			public void remove() {
                throw new UnsupportedOperationException();
			}
		};
	}
	
	public static final Iterator randomIterator(final ArrayList<byte[]> lookupHits, final int size, final int hitrate, final int minStrLen, final int maxStrLen, final char minChar, final char maxChar) {
		return new Iterator<Entry<byte[], byte[]>>() {
	        final Random generator = new Random();
            private int    count = 0;
            
            private String next = minChar + "";
            
            @Override
            public boolean hasNext() {
                return count < size;
            }
            
            @Override
            public Entry<byte[], byte[]> next() {
                
                count++;
                next = createNextString(next, minStrLen, maxStrLen, minChar, maxChar);

                if(generator.nextInt() % hitrate == 0)
                	lookupHits.add(next.getBytes());
                
                return new Entry<byte[], byte[]>() {
                    
                    final byte[] nextBytes = next.getBytes();
                    
                    @Override
                    public byte[] getKey() {
                        return nextBytes;
                    }
                    
                    @Override
                    public byte[] getValue() {
                        return nextBytes;
                    }
                    
                    @Override
                    public byte[] setValue(byte[] value) {
                        throw new UnsupportedOperationException();
                    }
                    
                };
            }
            
            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
            
        };
	}
	
	private static String createNextString(String st, int minStrLen, int maxStrLen, char minChar,
			char maxChar) {

		char[] chars = st.toCharArray();

		for (;;) {

			double rnd = Math.random();

			if (rnd < .1f + .8f / chars.length && chars.length < maxStrLen) {

				// append character
				char[] chars2 = new char[chars.length + 1];
				System.arraycopy(chars, 0, chars2, 0, chars.length);
				chars2[chars2.length - 1] = createRandomChar(minChar, maxChar);
				return new String(chars2);

			} else if (rnd > .95f + .05f / chars.length && chars.length > minStrLen) {

				int i = chars.length - 2;
				for (; i >= 0; i--)
					if (chars[i] < maxChar)
						break;

				if (i == -1)
					continue;

				// increment character and truncate
				char[] chars2 = new char[i + 1];
				System.arraycopy(chars, 0, chars2, 0, chars2.length);
				chars2[chars2.length - 1]++;
				return new String(chars2);

			} else if (chars[chars.length - 1] < maxChar) {

				// increment last character
				chars[chars.length - 1]++;
				return new String(chars);
			}
		}
	}

	public final static String createRandomString(char minChar, char maxChar, int minLength,
			int maxLength) {

		char[] chars = new char[(int) (Math.random() * (maxLength + 1)) + minLength];
		for (int i = 0; i < chars.length; i++)
			chars[i] = createRandomChar(minChar, maxChar);

		return new String(chars);
	}

	private static char createRandomChar(char minChar, char maxChar) {
		return (char) (Math.random() * (maxChar - minChar + 1) + minChar);
	}
}
