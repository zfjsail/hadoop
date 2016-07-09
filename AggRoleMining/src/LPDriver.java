
public class LPDriver {

    public static void main(String[] args) throws Exception {
        String[] forGB = {"", args[1] + "/Data0"};
        forGB[0] = args[0];
        LPBuilder.main(forGB);

        String[] forItr = {""};
        forItr[0] = args[1] + "/Data";
        LPIter.main(forItr);

        String[] forRV = {forItr[0] + "Final", args[1] + "/FinalRank"};
        LPViewer.main(forRV);
    }
}
