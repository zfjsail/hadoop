
public class AggDriver {
    public static void main(String[] args) throws Exception {
        /* pre-process */
        String[] forPre = {args[0], args[1] + "/seg"};
        PreProcess.main(forPre);

        /* gen-word-pair */
        String[] forWP = {args[1] + "/seg", args[1] + "/wp"};
        WordPairCounter.main(forWP);

        /* normalize word pair */
        String[] forNorWP = {args[1] + "/wp", args[1] + "/wp_norm"};
        Normalized.main(forNorWP);

        /* PageRank */
        String[] forPR = {args[1] + "/wp_norm/part-r-00000", args[1] + "/pr"};
        PageRank.main(forPR);

        /* label propagation */
        String[] forLP = {args[1] + "/wp_norm/part-r-00000", args[1] + "/lp"};
        LPDriver.main(forLP);
    }
}
