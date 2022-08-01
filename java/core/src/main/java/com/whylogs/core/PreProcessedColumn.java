package com.whylogs.core;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

// TODO: can we make this not have an init?
@Data
@NoArgsConstructor
public class PreProcessedColumn {
    /**
    * View of a column with data of various underlying storage.
    * we preprocess values into typed lists for downstream consumers.
    * We also track the null count and ensure that processed lists/Series don't contain null values.
     **/

    private ListView lists;
    private int nullCount = 0;
    private int boolCount = 0;
    private int boolCountWhereTrue = 0;
    private int length = -1;
    private Object originalColumn = null;


    // TODO: apply Scalars
    // TODO: apply Iterables and Iterators

    public static PreProcessedColumn apply(List data){
        PreProcessedColumn result = new PreProcessedColumn();
        result.setOriginalColumn(data);

        result.length = data.size();

        for(Object o : data){
            if(o == null){
                result.nullCount++;
            } else if(o instanceof Boolean){
                result.boolCount++;
                if((boolean)o){
                    result.boolCountWhereTrue++;
                }
            } else if(o instanceof Integer){
                result.lists.add((int)o);
            } else if(o instanceof String){
                result.lists.add((String)o);
            } else if(o instanceof Float){
                result.lists.add((float)o);
            } else {
                result.lists.add(o);
            }
        }
        return result;
    }
}
