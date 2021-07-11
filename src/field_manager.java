import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

public class field_manager {

    private HashMap<String, index_manager_v4_0_1> _field_map = new HashMap<String, index_manager_v4_0_1>();

    public final HashSet<Long> retrieve_field_union(final String field, final Vector<String> query) {
        if (_field_map.containsKey(field)) {
            return _field_map.get(field).retrieve_union(query);
        } else {
            return null;
        }
    }

    public final HashMap<Long, Vector<position_offset_t>> retrieve_field_intersection(final String field,
                                                                                      final Vector<String> query, final String query_line) {
        if (_field_map.containsKey(field)) {
            return _field_map.get(field).retrieve_intersection(query, query_line);
        } else {
            return null;
        }
    }

    void push_field_dir(String field_dir_path) throws IOException {
        File file = new File(field_dir_path);		//获取其file对象
        File[] fs = file.listFiles();	//遍历path下的文件和目录，放在File数组中
        for(File f:fs){					//遍历File[]数组
            if(!f.isDirectory()){//若非目录(即文件)，则打印
                index_manager_v4_0_1 im = new index_manager_v4_0_1();
                im.push_col_file(f.getPath());
                _field_map.put(f.getName() , im);
            }

        }
    }
}
