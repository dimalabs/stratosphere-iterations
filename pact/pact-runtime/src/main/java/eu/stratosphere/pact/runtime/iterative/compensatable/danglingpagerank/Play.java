package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank;

public class Play {

  public static void main(String[] args) {

//    byte[] bytes = "12 34 567 9000 10".getBytes();
//
    AsciiStringView view = new AsciiStringView();
//    view.set(bytes, 3, 14);


    byte[] bytes = "1 4 2 3                                                           ".getBytes();//<< (buffer length: 256, offset: 0, numBytes: 7
    view.set(bytes, 0, 7);

    System.out.println("#tokens:" + view.numTokens());

    while (view.nextToken()) {
      /*int size = view.length();
      for (int n = 0; n < size; n++) {
        System.out.print(view.charAt(n));
      }
      System.out.println();*/
      System.out.println(view.tokenAsLong());
    }

  }


}
