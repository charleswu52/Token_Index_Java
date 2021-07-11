import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

public class test_field_manager {
    static final String field = "query.txt";
    static final String field_dir = "./resources/field_dir";
    static final String query_path = "./resources/query.txt";
    static final String union_result_path = "./resources/union_result.txt";
    static final String intersection_result_path = "./resources/intersection_result.txt";


    void test_query(final field_manager manager, final String field, final Vector<Vector<String>> query_vec,
                    final Vector<String> query_line_vec, final boolean is_union, final boolean is_out)
    // std::ostream &os = std::cout
    {
        long begin_time = System.currentTimeMillis();
        for (int i = 0; i < query_vec.size(); ++i) {
            final Vector<String> query = query_vec.elementAt(i);
            final String query_line = query_line_vec.elementAt(i);
            // ?????
            long begin_time_in = System.currentTimeMillis();
            long set_size = 0;
            if (is_union) {
                final HashSet<Long> union_set = manager.retrieve_field_union(field, query);
                set_size = union_set.size();
                if (is_out)
                    System.out.println(query + ":" + union_set);
            } else {
                final HashMap<Long, Vector<position_offset_t>> intersection_set = manager
                        .retrieve_field_intersection(field, query, query_line);
//                System.out.println(intersection_set);
                set_size = intersection_set.size();
                if (is_out)
                    System.out.println(query + ":" + intersection_set);
            }
            long end_time_in = System.currentTimeMillis();
            long elapsed_time_in = end_time_in - begin_time_in;
            System.out.println("      Location time: " + elapsed_time_in + ", Result doc size: " + set_size);
        }
        long end_time = System.currentTimeMillis();
        long elapsed_time = end_time - begin_time;
        System.out.println("Location time: " + elapsed_time);
        double avg_time;
        if (query_vec.isEmpty())
            avg_time = 0.0;
        else
            avg_time = (double) elapsed_time / (double) query_vec.size();
        System.out.println("Average location time: " + avg_time);
    }

    void test_query_group(String field_dir, String field, String query_path,
            String union_result_path, String intersection_result_path) throws IOException {
        field_manager manager = new field_manager();
        manager.push_field_dir(field_dir);

        Object[] obj = load_query_vec(query_path);
        Vector<Vector<String>> query_vec = (Vector<Vector<String>>)obj[0];
        Vector<String> query_line_vec = (Vector<String>)obj[1];

//        std::ofstream ofs;

        System.out.println("field: " + field + ", union, cout ----------");
        test_query(manager, field, query_vec, query_line_vec, true, false);

        System.out.println("field: " + field + ", intersection, cout ----------");
        test_query(manager, field, query_vec, query_line_vec, false, false);

//        输出到文件
//        System.out.println("field: " + field + ", union, ofs ----------");
//        ofs.open(union_result_path, std::ofstream::out);
//        test_query(manager, field, query_vec, query_line_vec, true, true, ofs);
//        ofs.close();
//
//        std::cout << "field: " << field << ", intersection, ofs ----------" << std::endl;
//        ofs.open(intersection_result_path, std::ofstream::out);
//        test_query(manager, field, query_vec, query_line_vec, false, true, ofs);
//        ofs.close();
    }

//    std::tuple<std::vector<query_t>, std::vector<str_t>> load_query_vec(const path_t &path)
   Object[] load_query_vec(String path) throws IOException {
        Vector<Vector<String>> query_vec = new Vector<Vector<String>>();
        Vector<String> query_line_vec = new Vector<String>();


        BufferedReader filereader=new BufferedReader(new FileReader(path));
        String  line;
        while((line=filereader.readLine())!=null){
            line = line.toLowerCase();
            query_vec.add(line_to_query(line));
            query_line_vec.add(line);

        }

        filereader.close();
        Object[] obj = new Object[2];
        obj[0] = query_vec;
        obj[1] = query_line_vec;

        return obj;
    }

    Vector<String> line_to_query(String line)
    {
        Vector<String> query = new Vector<String>();
        String[] linearray = line.split(" ");
        for(String token : linearray){
            query.add(token);
        }
        return query;
    }

    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        new test_field_manager().test_query_group(field_dir, field, query_path, union_result_path, intersection_result_path);
//String s = "select last sl_1_tag7 as sl_1_tag7 from szsslkjyxgs_1ou7 autogen 68885b4c32ff4708a8a649c9b4baae59 where gatewayid 2c938083783650af01786535768406ac and time 2021 03 29t08 22 52z and time 2021 03 30t08 22 52z fill none \n";
//String s1 = s.substring(7,11);
//System.out.println(s1);
    }

}
