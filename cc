spark.sql(
      f"""
      MERGE INTO {target_tbl} TGT
      USING 
      (
        SELECT {tbl_cols},CAST(END_DTTM AS TIMESTAMP),
        HASH(CONCAT({change_cols})) AS SRC_CHK, FLG
        FROM
        (
          SELECT {tbl_cols}, END_DTTM,
          ROW_NUMBER() OVER(PARTITION BY HASH(CONCAT({change_cols})) ORDER BY END_DTTM DESC) AS toRW_NUMBER, FLG 
          FROM
          (
            SELECT {tbl_cols},
            IFNULL(LEAD(CAST(START_DTTM AS TIMESTAMP) - INTERVAL 1 SECOND) OVER(PARTITION BY {join_key} ORDER BY START_DTTM, FLG asc), '9999-12-31 00:00:00 UTC') AS END_DTTM, FLG 
            FROM
            (
              SELECT * EXCEPT(TGT_HASH) FROM (
                SELECT TGT.*, 'TGT' as FLG,1 as ROW_NUM FROM
                    (SELECT *,HASH(CONCAT({change_cols})) AS TGT_HASH from {target_tbl}) TGT
                INNER JOIN
                    (SELECT DISTINCT {join_key} FROM {source_tbl}) SRC
                ON {join_condition}
              ) WHERE TGT_HASH NOT IN (SELECT DISTINCT HASH(CONCAT({change_cols})) AS TGT_HASH FROM {source_tbl})
             
              UNION DISTINCT
              SELECT * FROM
                (
                  SELECT SRC_SUB.*, SRC_SUB.START_DTTM AS END_DTTM,'SRC' AS FLG,
                  ROW_NUMBER() OVER (PARTITION BY HASH(CONCAT({src_cols_sub}))ORDER BY SRC_SUB.START_DTTM ASC ) AS ROW_NUM -- All coulumns except audit cols
                  FROM {source_tbl} SRC_SUB
                  LEFT OUTER JOIN
                  {target_tbl} TGT_SUB
                  ON {join_condition_sub}
                  AND HASH(CONCAT({src_cols_sub})) = HASH(CONCAT({tgt_cols_sub}))
                  WHERE {sub_null_chk}
                ) WHERE ROW_NUM = 1
            )
          )
        ) WHERE ((RW_NUMBER=1 AND FLG='SRC') OR (FLG='TGT' AND RW_NUMBER = 1 AND END_DTTM <> '9999-12-31 00:00:00 UTC')) ORDER BY {join_key},START_DTTM ,END_DTTM ASC
      ) SRC
      ON {join_condition}
      AND SRC.START_DTTM=TGT.START_DTTM 
      AND (SRC.SRC_CHK = 
          HASH(CONCAT({tgt_change_cols})))
      AND SRC.END_DTTM<>TGT.END_DTTM 

      WHEN MATCHED THEN 
        UPDATE SET TGT.END_DTTM=SRC.END_DTTM 

      WHEN NOT MATCHED AND SRC.FLG='SRC' THEN 
        INSERT ({tbl_cols},END_DTTM)  
        VALUES ({tbl_cols},END_DTTM); 
    """)
