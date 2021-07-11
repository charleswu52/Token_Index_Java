import java.util.Comparator;

public class trpf_Comparator implements Comparator<token_relative_position_frequency_t> {

    @Override
    public int compare(token_relative_position_frequency_t o1, token_relative_position_frequency_t o2) {
        if(o1.frequency < o2.frequency){
            return -1;
        }else if(o1.frequency > o2.frequency){
            return 1;
        }else {
            if(o1.relative_position < o2.relative_position){
                return -1;
            }else if(o1.relative_position > o2.relative_position){
                return 1;
            }else {
                return 0;
            }
        }
//        return 0;
    }

}
