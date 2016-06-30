package org.xtreemfs.babudb.sandbox;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Map.Entry;

import org.xtreemfs.babudb.api.database.ResultSet;

public class DataGenerator {

	public static final Iterator xtreemfsIterator(final ArrayList<byte[]> lookupHits, final int hitrate, final String filename) throws IOException {
		final ArrayList<String> lines = new ArrayList<String>();
		String line = null;
        final Random generator = new Random();
        
		BufferedReader bf = new BufferedReader(new FileReader(filename));

		while((line = bf.readLine()) != null) {
			lines.add(line);
            if(generator.nextInt() % hitrate == 0)
            	lookupHits.add(line.getBytes());
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
	
	public static final ResultSet fileIterator(final ArrayList<byte[]> lookupHits, final int size, final int hitrate, final String filename) throws IOException {
		final ArrayList<String> lines = new ArrayList<String>();
		String line = null;
        final Random generator = new Random();
        int numLines = 0;
        
		BufferedReader bf = new BufferedReader(new FileReader(filename));

		while((line = bf.readLine()) != null) {
			lines.add(line);
			numLines++;
            if(generator.nextInt() % hitrate == 0)
            	lookupHits.add(line.getBytes());
		}

		List<String> sorted = null;
		if(size < numLines) {
			Collections.shuffle(lines);
		
			sorted = lines.subList(0, size);
			Collections.sort(sorted);
		} else {
			sorted = lines;
		}
		
		final Iterator<String> it = sorted.iterator();
		
		return new ResultSet() {
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
			
			@Override
			public void free() {
			    
			}
		};
	}
	
	public static final ResultSet randomIterator(final ArrayList<byte[]> lookupHits, final int size, final int hitrate, final int minStrLen, final int maxStrLen, final char minChar, final char maxChar) {
		return new ResultSet() {
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
            
            @Override
            public void free() {
                
            }
            
        };
	}
	
/*	private static byte[] fileID(String name) {
		
	} */
	
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
