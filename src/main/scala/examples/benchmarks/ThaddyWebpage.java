package examples.benchmarks;

public class ThaddyWebpage {


    public static String removeHttpPrefix(String str) {
        if (str.startsWith("https://")) {
            return str.substring(8);
        } else if (str.startsWith("http://")) {
            return str.substring(7);
        } else {
            return str;
        }
    }

    public static boolean webpageUIElementCompare(String websiteA, String websiteB, int heightA, int widthA, int xPosA, int yPosA, int heightB, int widthB, int xPosB, int yPosB) {

        String webpageNoPrefixA = removeHttpPrefix(websiteA);
        String webpageNoPrefixB = removeHttpPrefix(websiteB);

        if(webpageNoPrefixA != webpageNoPrefixB)
            return false;

        // Calculate the coordinates of the four corners of both rectangles
        int x1 = xPosA;
        int y1 = yPosA;
        int x2 = xPosA + widthA;
        int y2 = yPosA + heightA;
        int x3 = xPosB;
        int y3 = yPosB;
        int x4 = xPosB + widthB;
        int y4 = yPosB + heightB;

        // Check if the rectangles intersect by comparing the coordinates of their corners
        boolean intersects = (x1 < x4) && (x2 > x3) && (y1 < y4) && (y2 > y3);

        return intersects;
    }

    public static void main(String[] args) {
        System.out.println("Hello world!");

        // should return False
        boolean test1 = webpageUIElementCompare(
                "https://helloworld.com",
                "http://helloworld.com",
                100, 100, 200, 200,
                100, 100, 500, 500);

        // should return True
        boolean test2 = webpageUIElementCompare(
                "https://helloworld.com",
                "http://helloworld.com",
                200, 200, 100, 100,
                200, 200, 400, 400);
    }
}
