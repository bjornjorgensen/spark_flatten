
def test_(complex, sep="_"):
    keys_df = df.select(F.explode(F.map_keys(F.col(complex)))).distinct()
    keys = list(map(lambda row: row[0], keys_df.collect()))
    key_cols = list(map(lambda f: F.col(complex).getItem(f).alias(str(complex + sep + f)), keys))
    drop_column_list = [complex]
    final_cols = [col for col in df.columns if col not in drop_column_list] + key_cols
    return df.select(final_cols)



from pyspark.sql import types as T
import pyspark.sql.functions as F



def flatten(df, sep='_'):
    complex_fields = dict([
        (field.name, field.dataType) 
        for field in df.schema.fields 
        if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType) or isinstance(field.dataType, T.MapType)
    ])
    
    qualify = list(complex_fields.keys())[0] + "_"

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        
        if isinstance(complex_fields[col_name], T.StructType):
            expanded = [F.col(col_name + sep + k).alias(col_name + '_' + k) 
                        for k in [ n.name for n in complex_fields[col_name]]
                       ]
            
            df = df.select("*", *expanded).drop(col_name)
    
        elif isinstance(complex_fields[col_name], T.ArrayType): 
            df = df.withColumn(col_name, F.explode_outer(col_name))
        
        elif isinstance(complex_fields[col_name], T.MapType):
            keys_df = df.select(F.explode(F.map_keys(F.col(col_name)))).distinct()
            keys = list(map(lambda row: row[0], keys_df.collect()))
            key_cols = list(map(lambda f: F.col(col_name).getItem(f).alias(str(col_name + sep + f)), keys))
            drop_column_list = [col_name]
            #final_cols = [col_name for col_name in df.columns if col_name not in drop_column_list] + key_cols
            df = df.select([col_name for col_name in df.columns if col_name not in drop_column_list] + key_cols)
      
        complex_fields = dict([
            (field.name, field.dataType)
            for field in df.schema.fields
            if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType) or isinstance(field.dataType, T.MapType)
        ])
        
        
    for df_col_name in df.columns:
        df = df.withColumnRenamed(df_col_name, df_col_name.replace(qualify, ""))

    return df