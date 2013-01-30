package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank;

import com.google.common.base.Charsets;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AsciiStringViewTest {



  @Test
  public void testSimple() {
    long[] values = toLongArray("1 2 3", 0, 5);

    assertEquals(3, values.length);
    assertEquals(1l, values[0]);
    assertEquals(2l, values[1]);
    assertEquals(3l, values[2]);
  }

  private long[] toLongArray(String str, int offset, int numBytes) {
    byte[] bytes = str.getBytes(Charsets.US_ASCII);
    AsciiStringView view = new AsciiStringView();
    view.set(bytes, offset, numBytes);


    int numTokens = view.numTokens();
    System.out.println(numTokens);
    long[] values = new long[numTokens];

    for (int n = 0; n < numTokens; n++) {
      view.nextToken();
      values[n] = view.tokenAsLong();
    }
    return values;
  }


}
