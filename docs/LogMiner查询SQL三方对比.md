# LogMiner 查询 SQL 三方对比

对比对象：

- Debezium **1.9.8.Final** 上游 `LogMinerQueryBuilder.build`
- Debezium **2.7.4.Final** 上游 `LogMinerQueryBuilder.build`
- 本仓库二次开发后的 2.7.4 `LogMinerQueryBuilder.build`

线上从 `V$SQL` 抓到的语句是 1.9.8 骨架，表过滤被改成了 `SEG_OWNER = 'SG_MIS' AND TABLE_NAME IN (...)`。那份不在这三份 `build()` 里，放在文末对照。

## 统一配置

三份 `build()` 按下面同一套配置展开，方便逐行对照。

| 项 | 取值 |
| --- | --- |
| LOB | 关闭 |
| 缓冲 | `memory` |
| PDB | 未配置，SQL 里没有 `SRC_CON_NAME` |
| schema | `SG_MIS` |
| 表 | 线上那 85 张，见各段 SQL |
| 用户名包含/排除 | 未配置 |
| `store.only.captured.tables` | 关闭 |
| 2.7.4 过滤模式 | `log.mining.query.filter.mode=in` |

1.9.8 没有 `log.mining.query.filter.mode`。schema / 表包含列表在上游里写成 `REGEXP_LIKE`。配置值 `SG_MIS.CUST_AGRT` 里的点号不会被转义，拼进 SQL 后是 `^SG_MIS.CUST_AGRT$`，Oracle 正则里的 `.` 匹配任意一个字符。

两个绑定变量在源码里是 `?`。库里的游标把它们显示成 `:1`、`:2`。

未开 LOB 时，本仓库 2.7.4 与上游 2.7.4 的 `build()` 结果相同。二次开发只在 `lob.enabled=true` 且缓冲类型为 `memory` 时，给操作码条件追加 `OPERATION_CODE = 0`。这一支单独放在第 4 节，不混进未开 LOB 的对照。

## 1. Debezium 1.9.8.Final 上游

`SELECT` 到 `THREAD#` 为止。系统 schema 排除加在整句上。`START`(6)、`COMMIT`(7)、`MISSING_SCN`(34)、`ROLLBACK`(36) 单独成支，不套表过滤。DML 是 `1,2,3,255`。DDL 再排除 `SYS` / `SYSTEM` 和 `ORA_TEMP_%`。冲刷表写在 DML 支上。

```sql
SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION,
       USERNAME, ROW_ID, ROLLBACK, RS_ID, STATUS, INFO, SSN, THREAD#
FROM V$LOGMNR_CONTENTS
WHERE SCN > ? AND SCN <= ?
  AND (SEG_OWNER IS NULL OR SEG_OWNER NOT IN (
        'APPQOSSYS','AUDSYS','CTXSYS','DVSYS','DBSFWUSER','DBSNMP',
        'GSMADMIN_INTERNAL','LBACSYS','MDSYS','OJVMSYS','OLAPSYS','ORDDATA',
        'ORDSYS','OUTLN','SYS','SYSTEM','WMSYS','XDB'))
  AND (
        (OPERATION_CODE IN (6,7,34,36)
         OR (OPERATION_CODE = 5
             AND USERNAME NOT IN ('SYS','SYSTEM')
             AND INFO NOT LIKE 'INTERNAL DDL%'
             AND (TABLE_NAME IS NULL OR TABLE_NAME NOT LIKE 'ORA_TEMP_%')))
        OR
        (OPERATION_CODE IN (1,2,3,255)
         AND TABLE_NAME != 'LOG_MINING_FLUSH'
         AND (REGEXP_LIKE(SEG_OWNER,'^SG_MIS$','i'))
         AND (
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.CUST_AGRT$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.PLINE_DIST_STA_RELA$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.SRV_LOC_ELEC$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.INST_ELEC_VOU$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.IOT_POINT$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.ADM_REGN$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.CONN_GEN_POWER$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.STOP_RCVR_SUPL_EVNT$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.PIPELINE$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.CUST_ELEC_CONS_E_ATTR$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.SRV_LOC$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.ACQ_TRML$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.PREMISE$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.IT_RUN$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.SUPL_STOP$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.CHARG_REPLACE_FAC_SCH$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.DEV_FAC$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.DEV_RUN_REC$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.BID_WINN_RSLT$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.RS_APP_SCH$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.CHARG_PILE$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.PLCY_EXEC_RNG$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.SRV_LOC_GEN_ELEC$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.GPC$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.GEN_CONS_RELA$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.INST_RELA$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.PLANT$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.INST_EXTN_ATTR$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.CUST_IRRI_DRA_FAC$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.CNTRL_STA_PLINE_RELA$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.CUST_IRRI_DRA_FAC_APP$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.GC_APP_VOU$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.CUST_IRRI_DRA_FAC_VOU$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.CTLG_ADDL_PRC$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.TL_PARA_STD$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.BILG_STD$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.CTLG_PRC$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.CTLG_PRC_DTL$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.CUST_CONTACT$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.MR_DATA$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.IO_TCARD$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.ARR_BATCH_DEV_PARA$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.ARR_BATCH$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.CNTRL_STA$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.BILG_UNIT$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.DT_CUST$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.DEV_INST_RMV_READ$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.CALC_MEAS_POINT$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.CALC_MEAS_CONFIG$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.MEAS_POINT$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.BDATAGC_TAG_RESULT$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.HPLC_LIST_SUPPLY$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.SW$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.DEV_PUR_CTRT$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.REG_INIT_REC$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.PRODUCE_MF$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.COMM_MOD_CHIP_ID_INFO$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.STD_ADDR$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.CODE_CLS$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.GRID_USAGE$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.GRID$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.GRID_DTL$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.PIPE_LINE_RELA$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.CODE_CLS_VAL$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.GRID_STF$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.COMM_DEV$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.ISU_METER_RUN$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.ISU_ASSET$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.SW_DEV$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.ISU_REG_READ$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.ISU_CONS_CUST$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.SUPPLIER$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.IMPT_ELEC_CUST_ARCH$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.ISU_INST$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.BILG_RS_RCPT$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.CTRT_FULFL_WK_ORDER$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.DEV_PARA$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.INST_MEASUNIT$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.MR_UNIT$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.PAYBL_QTY_CHARG$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.PICK_BASIC_ERR$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.RS_INST_USAGE_QTY$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.TBL_TL_ELEC_QTY_STD$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.UNIT$','i') OR
               REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME,'^SG_MIS.UNIT_INST_RELA$','i')
             ))
      )
```

## 2. Debezium 2.7.4.Final 上游

`filter.mode=in`，内存缓冲，未开 LOB。`START`(6) 已去掉。`COMMIT` 靠 `TABLE_NAME IS NULL` 和 `SEG_OWNER IS NULL` 留下来。配了 `schema.include.list` 后，不再使用第 1 节那份系统 schema `NOT IN`，改成 `IN ('UNKNOWN','SG_MIS')`。多了 `DATA_OBJ#`、`DATA_OBJV#`、`DATA_OBJD#`。DDL 只剩 `OPERATION_CODE = 5 AND INFO NOT LIKE 'INTERNAL DDL%'`。冲刷表不在这句 SQL 里。

```sql
SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION,
       USERNAME, ROW_ID, ROLLBACK, RS_ID, STATUS, INFO, SSN, THREAD#,
       DATA_OBJ#, DATA_OBJV#, DATA_OBJD#
FROM V$LOGMNR_CONTENTS
WHERE SCN > ? AND SCN <= ?
  AND ((
        (OPERATION_CODE IN (1,2,3,7,34,36,255))
        AND (SEG_OWNER IS NULL OR UPPER(SEG_OWNER) IN ('UNKNOWN','SG_MIS'))
        AND (TABLE_NAME IS NULL
             OR TABLE_NAME LIKE 'OBJ#%'
             OR UPPER(SEG_OWNER || '.' || TABLE_NAME) IN (
                  'SG_MIS.CUST_AGRT',
                  'SG_MIS.PLINE_DIST_STA_RELA',
                  'SG_MIS.SRV_LOC_ELEC',
                  'SG_MIS.INST_ELEC_VOU',
                  'SG_MIS.IOT_POINT',
                  'SG_MIS.ADM_REGN',
                  'SG_MIS.CONN_GEN_POWER',
                  'SG_MIS.STOP_RCVR_SUPL_EVNT',
                  'SG_MIS.PIPELINE',
                  'SG_MIS.CUST_ELEC_CONS_E_ATTR',
                  'SG_MIS.SRV_LOC',
                  'SG_MIS.ACQ_TRML',
                  'SG_MIS.PREMISE',
                  'SG_MIS.IT_RUN',
                  'SG_MIS.SUPL_STOP',
                  'SG_MIS.CHARG_REPLACE_FAC_SCH',
                  'SG_MIS.DEV_FAC',
                  'SG_MIS.DEV_RUN_REC',
                  'SG_MIS.BID_WINN_RSLT',
                  'SG_MIS.RS_APP_SCH',
                  'SG_MIS.CHARG_PILE',
                  'SG_MIS.PLCY_EXEC_RNG',
                  'SG_MIS.SRV_LOC_GEN_ELEC',
                  'SG_MIS.GPC',
                  'SG_MIS.GEN_CONS_RELA',
                  'SG_MIS.INST_RELA',
                  'SG_MIS.PLANT',
                  'SG_MIS.INST_EXTN_ATTR',
                  'SG_MIS.CUST_IRRI_DRA_FAC',
                  'SG_MIS.CNTRL_STA_PLINE_RELA',
                  'SG_MIS.CUST_IRRI_DRA_FAC_APP',
                  'SG_MIS.GC_APP_VOU',
                  'SG_MIS.CUST_IRRI_DRA_FAC_VOU',
                  'SG_MIS.CTLG_ADDL_PRC',
                  'SG_MIS.TL_PARA_STD',
                  'SG_MIS.BILG_STD',
                  'SG_MIS.CTLG_PRC',
                  'SG_MIS.CTLG_PRC_DTL',
                  'SG_MIS.CUST_CONTACT',
                  'SG_MIS.MR_DATA',
                  'SG_MIS.IO_TCARD',
                  'SG_MIS.ARR_BATCH_DEV_PARA',
                  'SG_MIS.ARR_BATCH',
                  'SG_MIS.CNTRL_STA',
                  'SG_MIS.BILG_UNIT',
                  'SG_MIS.DT_CUST',
                  'SG_MIS.DEV_INST_RMV_READ',
                  'SG_MIS.CALC_MEAS_POINT',
                  'SG_MIS.CALC_MEAS_CONFIG',
                  'SG_MIS.MEAS_POINT',
                  'SG_MIS.BDATAGC_TAG_RESULT',
                  'SG_MIS.HPLC_LIST_SUPPLY',
                  'SG_MIS.SW',
                  'SG_MIS.DEV_PUR_CTRT',
                  'SG_MIS.REG_INIT_REC',
                  'SG_MIS.PRODUCE_MF',
                  'SG_MIS.COMM_MOD_CHIP_ID_INFO',
                  'SG_MIS.STD_ADDR',
                  'SG_MIS.CODE_CLS',
                  'SG_MIS.GRID_USAGE',
                  'SG_MIS.GRID',
                  'SG_MIS.GRID_DTL',
                  'SG_MIS.PIPE_LINE_RELA',
                  'SG_MIS.CODE_CLS_VAL',
                  'SG_MIS.GRID_STF',
                  'SG_MIS.COMM_DEV',
                  'SG_MIS.ISU_METER_RUN',
                  'SG_MIS.ISU_ASSET',
                  'SG_MIS.SW_DEV',
                  'SG_MIS.ISU_REG_READ',
                  'SG_MIS.ISU_CONS_CUST',
                  'SG_MIS.SUPPLIER',
                  'SG_MIS.IMPT_ELEC_CUST_ARCH',
                  'SG_MIS.ISU_INST',
                  'SG_MIS.BILG_RS_RCPT',
                  'SG_MIS.CTRT_FULFL_WK_ORDER',
                  'SG_MIS.DEV_PARA',
                  'SG_MIS.INST_MEASUNIT',
                  'SG_MIS.MR_UNIT',
                  'SG_MIS.PAYBL_QTY_CHARG',
                  'SG_MIS.PICK_BASIC_ERR',
                  'SG_MIS.RS_INST_USAGE_QTY',
                  'SG_MIS.TBL_TL_ELEC_QTY_STD',
                  'SG_MIS.UNIT',
                  'SG_MIS.UNIT_INST_RELA'
             ))
      ) OR (OPERATION_CODE = 5 AND INFO NOT LIKE 'INTERNAL DDL%'))
```

## 3. 本仓库二次开发后的 2.7.4，未开 LOB

与第 2 节逐字相同。

`LogMinerQueryBuilder.getOperationCodePredicate` 里追加 `OPERATION_CODE = 0` 的条件被 `isLobEnabled()` 和 `LogMiningBufferType.MEMORY` 同时卡住。未开 LOB 时这段不会进入 SQL。

## 4. 本仓库二次开发后的 2.7.4，开启 LOB 且 memory 缓冲

这是三方 `build()` 里唯一和上游 2.7.4 不同的句子。操作码变为 `1,2,3,6,7,9,10,11,29,34,36,68,70,71,255`，并多一段 INTERNAL 行。上游 2.7.4 在同样配置下没有 `OPERATION_CODE = 0` 这一支，其余列、SCN、schema、表 `IN`、DDL 与下面相同。

```sql
SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION,
       USERNAME, ROW_ID, ROLLBACK, RS_ID, STATUS, INFO, SSN, THREAD#,
       DATA_OBJ#, DATA_OBJV#, DATA_OBJD#
FROM V$LOGMNR_CONTENTS
WHERE SCN > ? AND SCN <= ?
  AND ((
        (OPERATION_CODE IN (1,2,3,6,7,9,10,11,29,34,36,68,70,71,255)
         OR (OPERATION_CODE = 0
             AND ROLLBACK = 0
             AND ROW_ID NOT LIKE '%AAAAAAAAAAAA'
             AND SEQUENCE# > 1))
        AND (SEG_OWNER IS NULL OR UPPER(SEG_OWNER) IN ('UNKNOWN','SG_MIS'))
        AND (TABLE_NAME IS NULL
             OR TABLE_NAME LIKE 'OBJ#%'
             OR UPPER(SEG_OWNER || '.' || TABLE_NAME) IN (
                  'SG_MIS.CUST_AGRT',
                  'SG_MIS.PLINE_DIST_STA_RELA',
                  'SG_MIS.SRV_LOC_ELEC',
                  'SG_MIS.INST_ELEC_VOU',
                  'SG_MIS.IOT_POINT',
                  'SG_MIS.ADM_REGN',
                  'SG_MIS.CONN_GEN_POWER',
                  'SG_MIS.STOP_RCVR_SUPL_EVNT',
                  'SG_MIS.PIPELINE',
                  'SG_MIS.CUST_ELEC_CONS_E_ATTR',
                  'SG_MIS.SRV_LOC',
                  'SG_MIS.ACQ_TRML',
                  'SG_MIS.PREMISE',
                  'SG_MIS.IT_RUN',
                  'SG_MIS.SUPL_STOP',
                  'SG_MIS.CHARG_REPLACE_FAC_SCH',
                  'SG_MIS.DEV_FAC',
                  'SG_MIS.DEV_RUN_REC',
                  'SG_MIS.BID_WINN_RSLT',
                  'SG_MIS.RS_APP_SCH',
                  'SG_MIS.CHARG_PILE',
                  'SG_MIS.PLCY_EXEC_RNG',
                  'SG_MIS.SRV_LOC_GEN_ELEC',
                  'SG_MIS.GPC',
                  'SG_MIS.GEN_CONS_RELA',
                  'SG_MIS.INST_RELA',
                  'SG_MIS.PLANT',
                  'SG_MIS.INST_EXTN_ATTR',
                  'SG_MIS.CUST_IRRI_DRA_FAC',
                  'SG_MIS.CNTRL_STA_PLINE_RELA',
                  'SG_MIS.CUST_IRRI_DRA_FAC_APP',
                  'SG_MIS.GC_APP_VOU',
                  'SG_MIS.CUST_IRRI_DRA_FAC_VOU',
                  'SG_MIS.CTLG_ADDL_PRC',
                  'SG_MIS.TL_PARA_STD',
                  'SG_MIS.BILG_STD',
                  'SG_MIS.CTLG_PRC',
                  'SG_MIS.CTLG_PRC_DTL',
                  'SG_MIS.CUST_CONTACT',
                  'SG_MIS.MR_DATA',
                  'SG_MIS.IO_TCARD',
                  'SG_MIS.ARR_BATCH_DEV_PARA',
                  'SG_MIS.ARR_BATCH',
                  'SG_MIS.CNTRL_STA',
                  'SG_MIS.BILG_UNIT',
                  'SG_MIS.DT_CUST',
                  'SG_MIS.DEV_INST_RMV_READ',
                  'SG_MIS.CALC_MEAS_POINT',
                  'SG_MIS.CALC_MEAS_CONFIG',
                  'SG_MIS.MEAS_POINT',
                  'SG_MIS.BDATAGC_TAG_RESULT',
                  'SG_MIS.HPLC_LIST_SUPPLY',
                  'SG_MIS.SW',
                  'SG_MIS.DEV_PUR_CTRT',
                  'SG_MIS.REG_INIT_REC',
                  'SG_MIS.PRODUCE_MF',
                  'SG_MIS.COMM_MOD_CHIP_ID_INFO',
                  'SG_MIS.STD_ADDR',
                  'SG_MIS.CODE_CLS',
                  'SG_MIS.GRID_USAGE',
                  'SG_MIS.GRID',
                  'SG_MIS.GRID_DTL',
                  'SG_MIS.PIPE_LINE_RELA',
                  'SG_MIS.CODE_CLS_VAL',
                  'SG_MIS.GRID_STF',
                  'SG_MIS.COMM_DEV',
                  'SG_MIS.ISU_METER_RUN',
                  'SG_MIS.ISU_ASSET',
                  'SG_MIS.SW_DEV',
                  'SG_MIS.ISU_REG_READ',
                  'SG_MIS.ISU_CONS_CUST',
                  'SG_MIS.SUPPLIER',
                  'SG_MIS.IMPT_ELEC_CUST_ARCH',
                  'SG_MIS.ISU_INST',
                  'SG_MIS.BILG_RS_RCPT',
                  'SG_MIS.CTRT_FULFL_WK_ORDER',
                  'SG_MIS.DEV_PARA',
                  'SG_MIS.INST_MEASUNIT',
                  'SG_MIS.MR_UNIT',
                  'SG_MIS.PAYBL_QTY_CHARG',
                  'SG_MIS.PICK_BASIC_ERR',
                  'SG_MIS.RS_INST_USAGE_QTY',
                  'SG_MIS.TBL_TL_ELEC_QTY_STD',
                  'SG_MIS.UNIT',
                  'SG_MIS.UNIT_INST_RELA'
             ))
      ) OR (OPERATION_CODE = 5 AND INFO NOT LIKE 'INTERNAL DDL%'))
```

对应源码：

```text
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-oracle-cdc/src/main/java/io/debezium/connector/oracle/logminer/LogMinerQueryBuilder.java
```

`getOperationCodePredicate` 在 LOB + memory 时追加：

```sql
OR (OPERATION_CODE = 0 AND ROLLBACK = 0 AND ROW_ID NOT LIKE '%AAAAAAAAAAAA' AND SEQUENCE# > 1)
```

## 5. 线上抓到的 1.9.8 二次开发 SQL

结构与第 1 节相同，表过滤从 `REGEXP_LIKE(SEG_OWNER || '.' || TABLE_NAME, ...)` 改成了 `SEG_OWNER = 'SG_MIS' AND TABLE_NAME IN (...)`。`COMMIT` 仍在 `OPERATION_CODE IN (6,7,34,36)` 里，不受这 85 张表限制。

```sql
SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF,
       TABLE_NAME, SEG_OWNER, OPERATION, USERNAME, ROW_ID, ROLLBACK, RS_ID,
       STATUS, INFO, SSN, THREAD#
FROM V$LOGMNR_CONTENTS
WHERE SCN > :1
  AND SCN <= :2
  AND (SEG_OWNER IS NULL OR SEG_OWNER NOT IN (
        'APPQOSSYS','AUDSYS','CTXSYS','DVSYS','DBSFWUSER','DBSNMP',
        'GSMADMIN_INTERNAL','LBACSYS','MDSYS','OJVMSYS','OLAPSYS','ORDDATA',
        'ORDSYS','OUTLN','SYS','SYSTEM','WMSYS','XDB'))
  AND (
        (OPERATION_CODE IN (6,7,34,36)
         OR (OPERATION_CODE = 5
             AND USERNAME NOT IN ('SYS','SYSTEM')
             AND INFO NOT LIKE 'INTERNAL DDL%'
             AND (TABLE_NAME IS NULL OR TABLE_NAME NOT LIKE 'ORA_TEMP_%')))
        OR
        (OPERATION_CODE IN (1,2,3,255)
         AND TABLE_NAME != 'LOG_MINING_FLUSH'
         AND SEG_OWNER = 'SG_MIS'
         AND TABLE_NAME IN (
              'CUST_AGRT',
              'PLINE_DIST_STA_RELA',
              'SRV_LOC_ELEC',
              'INST_ELEC_VOU',
              'IOT_POINT',
              'ADM_REGN',
              'CONN_GEN_POWER',
              'STOP_RCVR_SUPL_EVNT',
              'PIPELINE',
              'CUST_ELEC_CONS_E_ATTR',
              'SRV_LOC',
              'ACQ_TRML',
              'PREMISE',
              'IT_RUN',
              'SUPL_STOP',
              'CHARG_REPLACE_FAC_SCH',
              'DEV_FAC',
              'DEV_RUN_REC',
              'BID_WINN_RSLT',
              'RS_APP_SCH',
              'CHARG_PILE',
              'PLCY_EXEC_RNG',
              'SRV_LOC_GEN_ELEC',
              'GPC',
              'GEN_CONS_RELA',
              'INST_RELA',
              'PLANT',
              'INST_EXTN_ATTR',
              'CUST_IRRI_DRA_FAC',
              'CNTRL_STA_PLINE_RELA',
              'CUST_IRRI_DRA_FAC_APP',
              'GC_APP_VOU',
              'CUST_IRRI_DRA_FAC_VOU',
              'CTLG_ADDL_PRC',
              'TL_PARA_STD',
              'BILG_STD',
              'CTLG_PRC',
              'CTLG_PRC_DTL',
              'CUST_CONTACT',
              'MR_DATA',
              'IO_TCARD',
              'ARR_BATCH_DEV_PARA',
              'ARR_BATCH',
              'CNTRL_STA',
              'BILG_UNIT',
              'DT_CUST',
              'DEV_INST_RMV_READ',
              'CALC_MEAS_POINT',
              'CALC_MEAS_CONFIG',
              'MEAS_POINT',
              'BDATAGC_TAG_RESULT',
              'HPLC_LIST_SUPPLY',
              'SW',
              'DEV_PUR_CTRT',
              'REG_INIT_REC',
              'PRODUCE_MF',
              'COMM_MOD_CHIP_ID_INFO',
              'STD_ADDR',
              'CODE_CLS',
              'GRID_USAGE',
              'GRID',
              'GRID_DTL',
              'PIPE_LINE_RELA',
              'CODE_CLS_VAL',
              'GRID_STF',
              'COMM_DEV',
              'ISU_METER_RUN',
              'ISU_ASSET',
              'SW_DEV',
              'ISU_REG_READ',
              'ISU_CONS_CUST',
              'SUPPLIER',
              'IMPT_ELEC_CUST_ARCH',
              'ISU_INST',
              'BILG_RS_RCPT',
              'CTRT_FULFL_WK_ORDER',
              'DEV_PARA',
              'INST_MEASUNIT',
              'MR_UNIT',
              'PAYBL_QTY_CHARG',
              'PICK_BASIC_ERR',
              'RS_INST_USAGE_QTY',
              'TBL_TL_ELEC_QTY_STD',
              'UNIT',
              'UNIT_INST_RELA'
         )
        )
      )
```

## 差异摘要

| 项 | 1.9.8 上游 | 2.7.4 上游 | 本仓库 2.7.4 | 线上 1.9.8 |
| --- | --- | --- | --- | --- |
| 列 | 到 `THREAD#` | 多 `DATA_OBJ#`、`DATA_OBJV#`、`DATA_OBJD#` | 同上游 2.7.4 | 同 1.9.8 上游 |
| `START`(6) | 始终包含，且在表过滤之外 | 未开 LOB 的 memory 缓冲去掉 | 同上游 2.7.4 | 同 1.9.8 上游 |
| `COMMIT`(7) | 在表过滤之外 | `TABLE_NAME IS NULL` 放行 | 同上游 2.7.4 | 在表过滤之外 |
| 表条件 | `REGEXP_LIKE` | `UPPER(SEG_OWNER \|\| '.' \|\| TABLE_NAME) IN` | 同上游 2.7.4 | `SEG_OWNER =` 且 `TABLE_NAME IN` |
| 系统 schema | 整句 `NOT IN` | 有 schema 包含列表时改为 `IN ('UNKNOWN','SG_MIS')` | 同上游 2.7.4 | 整句 `NOT IN` |
| PDB | 只限制 DML/DDL。本配置未配 PDB | 配了 PDB 就限制整句，含 `COMMIT` | 同上游 2.7.4 | 未出现 |
| DDL | 排除 `SYS`/`SYSTEM` 和 `ORA_TEMP_%` | 只排除 `INTERNAL DDL` | 同上游 2.7.4 | 同 1.9.8 上游 |
| 冲刷表 | SQL 排除 `LOG_MINING_FLUSH` | SQL 不写，Java 侧丢弃 | 同上游 2.7.4 | SQL 排除 |
| `OPERATION_CODE = 0` | 无 | 无 | 仅 LOB + memory | 无 |