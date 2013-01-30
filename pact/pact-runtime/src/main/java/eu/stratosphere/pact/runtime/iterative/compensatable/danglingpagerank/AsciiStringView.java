package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank;

public class AsciiStringView {

  private byte[] buffer;

  private int startIndex;
  private int endIndex;

  private int tokenStartIndex;
  private int tokenEndIndex;

  private static final int RADIX_TEN = 10;
  private static final long MULTMIN_RADIX_TEN = Long.MIN_VALUE / 10;
  private static final long N_MULTMAX_RADIX_TEN = -Long.MAX_VALUE / 10;

  public void set(byte[] buffer, int offset, int numBytes) {
    this.buffer = buffer;
    tokenEndIndex = offset - 1;
    startIndex = offset;
    endIndex = offset + numBytes;
  }

  public int numTokens() {
    int matches = 0;
    for (int n = startIndex; n < endIndex; n++) {
      if (Character.isWhitespace((char)buffer[n])) {
        matches++;
      }
    }
    return matches + 1;
  }

  public boolean nextToken() {

    if (tokenEndIndex >= endIndex - 1) {
      return false;
    }

    tokenStartIndex = tokenEndIndex + 1;
    tokenEndIndex = tokenStartIndex;
    //System.out.println(tokenEndIndex + " --> " + ((char)buffer[tokenEndIndex]));
    while (true) {
      try {
        ++tokenEndIndex;
//        System.out.println(tokenEndIndex + " --> \"" + ((char)buffer[tokenEndIndex]) + "\", " + tokenStartIndex + "-" + tokenEndIndex);
        if (tokenEndIndex >= endIndex - 1) {
          return false;
        } else if (Character.isWhitespace((char)buffer[tokenEndIndex])) {
//          System.out.println("BREAK");
          break;
        }
      } catch (ArrayIndexOutOfBoundsException e) {
        //System.err.println();
        throw new RuntimeException("Error tokenizing: " + asString(), e);
      }
    }
    return true;
  }


  public int tokenLength() {
    return tokenEndIndex - tokenStartIndex;
  }

  public char tokenCharAt(int index) {
    return (char) buffer[tokenStartIndex + index];
  }

  public long tokenAsLong() {

    long result = 0;
    boolean negative = false;
    int i = 0, max = tokenLength();
    long limit;
    long multmin;
    int digit;

    if (max > 0) {
      if (tokenCharAt(0) == '-') {
        negative = true;
        limit = Long.MIN_VALUE;
        i++;
      } else {
        limit = -Long.MAX_VALUE;
      }

      multmin = negative ? MULTMIN_RADIX_TEN : N_MULTMAX_RADIX_TEN;

      if (i < max) {
        digit = Character.digit(tokenCharAt(i++), RADIX_TEN);
        if (digit < 0) {
          throw new NumberFormatException();
        } else {
          result = -digit;
        }
      }
      while (i < max) {
        // Accumulating negatively avoids surprises near MAX_VALUE
        digit = Character.digit(tokenCharAt(i++), RADIX_TEN);
        if (digit < 0) {
          throw new NumberFormatException(asString());
        }
        if (result < multmin) {
          throw new NumberFormatException(asString());
        }
        result *= RADIX_TEN;
        if (result < limit + digit) {
          throw new NumberFormatException(asString());
        }
        result -= digit;
      }
    } else {
      throw new NumberFormatException(asString());
    }
    if (negative) {
      if (i > 1) {
        return result;
      } else { /* Only got "-" */
        throw new NumberFormatException(asString());
      }
    } else {
      return -result;
    }
  }

  private String asString() {
    return ">>" + new String(buffer, startIndex, endIndex - startIndex) + "<< (buffer length: " + buffer.length +", offset: " + startIndex + ", numBytes: " + (endIndex - startIndex)+ ")";
  }

}
