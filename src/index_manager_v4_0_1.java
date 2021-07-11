import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;


public class index_manager_v4_0_1 {
    private HashMap<String, HashMap<Long, Vector<position_offset_t>>> _inverted_index = new HashMap<String, HashMap<Long, Vector<position_offset_t>>>();
    private HashMap<Long, String> _doc_line_index = new HashMap<Long, String>();
    static final long low_frequency = 10;

    void push_col_file(String col_file_path) throws IOException {

        BufferedReader filereader=new BufferedReader(new FileReader(col_file_path));
        String  str;
        while((str=filereader.readLine())!=null){
            str = str.toLowerCase();
            push_doc_line(str);
        }

        filereader.close();
    }

    void push_doc_line(String doc_line)
    {
        String token;
        String doc_id_string = doc_line.split(" ")[0];
        Long doc_id = Long.valueOf(doc_id_string);
        String new_line = doc_line.substring(doc_id_string.length()+1 , doc_line.length());
        _doc_line_index.put(doc_id, new_line);


        long position = 0;
        int begin = 0, end = 0;
        boolean is_find_begin = false;
        for (int i = 0; i < new_line.length(); ++i)
        {
            final char ch = new_line.charAt(i);
            if (ch != ' ')
            {
                if (!is_find_begin)
                {
                    is_find_begin = true;
                    begin = i;
                }
                end = i;
            }
            else
            {
                if (is_find_begin)
                {
//                    System.out.println("begin  :" + begin);
//                    System.out.println("end  :" + end);
                    is_find_begin = false;
                    token = new_line.substring(begin, end + 1);

                    if(!_inverted_index.containsKey(token)){
                        _inverted_index.put(token , new HashMap<Long, Vector<position_offset_t>>());

                    }

                    HashMap<Long, Vector<position_offset_t>> doc_id_map = _inverted_index.get(token);
                    if(!doc_id_map.containsKey(doc_id)){
                        doc_id_map.put(doc_id , new Vector<position_offset_t>());
                    }
                    Vector<position_offset_t> position_offset_vec = doc_id_map.get(doc_id);

                    position_offset_vec.add(new position_offset_t(position++ , new offset_t(begin , end)));
                }
            }
        }
    }


    public final HashSet<Long> retrieve_union(final Vector<String> query) {
        HashSet<Long> union_set = new HashSet<Long>();
        for (int i = 0; i < query.size(); i++) {
            String token = query.elementAt(i);
            if (_inverted_index.containsKey(token)) {
                HashMap<Long, Vector<position_offset_t>> inverted_index_iter = _inverted_index.get(token);
                for (Long key : inverted_index_iter.keySet()) {
                    union_set.add(key);
                }
            } else {
                continue;
            }
        }
        return union_set;
    }

    public final HashMap<Long, Vector<position_offset_t>> retrieve_intersection(final Vector<String> query,
                                                                                final String query_line) {
        Vector<token_relative_position_frequency_t> token_relative_position_frequency_vec = this
                .gen_token_relative_position_frequency_vec(query);
        return not_low_frequency_retrieve_intersection(token_relative_position_frequency_vec);
    }

    public final Vector<token_relative_position_frequency_t> gen_token_relative_position_frequency_vec(
            final Vector<String> query) {
        Vector<token_relative_position_frequency_t> token_relative_position_frequency_vec = new Vector<token_relative_position_frequency_t>();
        for (long relative_position = 0; relative_position < query.size(); ++relative_position) {
            final String token = query.elementAt((int) relative_position);
            long frequency = calc_frequency(token);
            token_relative_position_frequency_vec
                    .add(new token_relative_position_frequency_t(token, relative_position, frequency));
        }
        Comparator<token_relative_position_frequency_t> com = new trpf_Comparator();
        Collections.sort(token_relative_position_frequency_vec, com);
        return token_relative_position_frequency_vec;
    }

    public final long calc_frequency(final String token) {
        if (_inverted_index.containsKey(token)) {
            HashMap<Long, Vector<position_offset_t>> inverted_index_iter = _inverted_index.get(token);
            long frequency = 0;
            frequency += inverted_index_iter.size();
            return frequency;
        } else {
            return 0;
        }
    }

    public final HashMap<Long, Vector<position_offset_t>> not_low_frequency_retrieve_intersection(
            Vector<token_relative_position_frequency_t> token_relative_position_frequency_vec) {
        final token_relative_position_frequency_t first_token_relative_position_frequency = token_relative_position_frequency_vec
                .elementAt(0);
        final String first_token = first_token_relative_position_frequency.token;
        final long first_relative_position = first_token_relative_position_frequency.relative_position;
        if (!_inverted_index.containsKey(first_token)) {
            return null;
        }
        HashMap<Long, Vector<position_offset_t>> intersection_doc_id_map = _inverted_index.get(first_token);
        for (int i = 1; i < token_relative_position_frequency_vec.size(); ++i) {
            final token_relative_position_frequency_t token_relative_position_frequency = token_relative_position_frequency_vec
                    .elementAt(i);
            final String token = first_token_relative_position_frequency.token;
            final long relative_position = first_token_relative_position_frequency.relative_position;
            if (!_inverted_index.containsKey(token)) {
                return null;
            }
            final HashMap<Long, Vector<position_offset_t>> doc_id_map = _inverted_index.get(token);
            Iterator it = intersection_doc_id_map.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Long, Vector<position_offset_t>> entry = (Entry<Long, Vector<position_offset_t>>) it.next();
                final Long doc_id = entry.getKey();
                if (!doc_id_map.containsKey(doc_id)) {
                    intersection_doc_id_map.remove(doc_id);
                    continue;
                }
                Vector<position_offset_t> intersection_position_offset_vec = entry.getValue();
                final Vector<position_offset_t> position_offset_vec = doc_id_map.get(doc_id);
                long quick = 0;
                long slow = 0;
                for (; quick < intersection_position_offset_vec.size(); ++quick) {
                    final position_offset_t intersection_position_offset = intersection_position_offset_vec
                            .elementAt((int) quick);
                    final long position = intersection_position_offset.position;
                    for (final position_offset_t position_offset : position_offset_vec) {
                        if (position - first_relative_position + relative_position == position_offset.position) {
                            position_offset_t temp_position_offset = intersection_position_offset_vec
                                    .elementAt((int) slow);
                            temp_position_offset.position = position;
                            if (intersection_position_offset.offset.begin < temp_position_offset.offset.begin)
                                temp_position_offset.offset.begin = intersection_position_offset.offset.begin;
                            if (intersection_position_offset.offset.end > temp_position_offset.offset.end)
                                temp_position_offset.offset.end = position_offset.offset.end;
                            ++slow;
                            break;
                        }
                    }
                }
                if (slow == 0) {
                    intersection_doc_id_map.remove(doc_id);
                    continue;
                }
                // C++ resize
            }
            if (intersection_doc_id_map.isEmpty()) {
                return null;
            }
        }

        HashMap<Long, Vector<position_offset_t>> temp_doc_id_map = new HashMap<Long, Vector<position_offset_t>>();
        // HashMap<Long,Vector<position_offset_t>>
        Iterator it = intersection_doc_id_map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Vector<position_offset_t>> entry = (Entry<Long, Vector<position_offset_t>>) it.next();
            final Long doc_id = entry.getKey();
            if (!temp_doc_id_map.containsKey(doc_id)) {
                temp_doc_id_map.put(doc_id, new Vector<position_offset_t>());
            }
            Vector<position_offset_t> temp_position_offset_vec = temp_doc_id_map.get(doc_id);
            Vector<position_offset_t> intersection_position_offset_vec = entry.getValue();
            for (final position_offset_t position_offset : intersection_position_offset_vec) {
                temp_position_offset_vec.add(new position_offset_t(position_offset.position - first_relative_position,
                        position_offset.offset));
            }

        }
        intersection_doc_id_map = temp_doc_id_map;

        return intersection_doc_id_map;
    }


}
