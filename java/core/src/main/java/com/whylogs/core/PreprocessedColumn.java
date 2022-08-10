package com.whylogs.core;

import lombok.*;

import java.util.Collection;

@Getter @Setter @EqualsAndHashCode
public class PreprocessedColumn {
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

    public PreprocessedColumn(){
        this.lists = new ListView();
    }

    // TODO: apply Scalars
    // TODO: apply Iterables and Iterators

    public static PreprocessedColumn apply(Collection<?> data){
        PreprocessedColumn result = new PreprocessedColumn();
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
            } else if(o instanceof Integer ){
                result.lists.add((int) o);
            } else if(o instanceof Short) {
                result.lists.add(((Short) o).intValue());
            } else if(o instanceof Byte){
                result.lists.add(((Byte) o).intValue());
            } else if(o instanceof String) {
                result.lists.add((String) o);
            } else if(o instanceof Character){
                result.lists.add(((Character) o).toString());
            } else if(o instanceof Double){
                result.lists.add((double) o);
            } else if(o instanceof Float){
                result.lists.add(((Float) o).doubleValue());
            } else {
                result.lists.add(o);
            }
        }
        return result;
    }

    public boolean hasListInts(){
        return !this.lists.getInts().isEmpty();
    }
}
