package parser

import (
	"fmt"
	format1 "github.com/daiguadaidai/parser/format"
	"strings"
	"testing"
)

func Test_Degester_01(t *testing.T) {
	query := "select * from b where id = 1"
	ps := New()
	stmt, err := ps.ParseOneStmt(query, "", "")
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(stmt.Text())
	normalized, digest := NormalizeDigest(stmt.Text())
	fmt.Println(normalized)
	fmt.Println(digest)

	var sb strings.Builder
	if err = stmt.Pretty(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb), 0, 0, ""); err != nil {
		t.Fatalf("Restore 出错. %s", err.Error())
	}

	fmt.Println("Restore语句:", sb.String())
}

func Test_Pretty_CreateTable_01(t *testing.T) {
	query := `
CREATE TABLE es_query_logs (
  id bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  username varchar(50) NOT NULL COMMENT '用户名',
  business varchar(100) NOT NULL COMMENT '业务线',
  service varchar(100) NOT NULL COMMENT '服务',
  cluster_name varchar(190) NOT NULL COMMENT 'PHBase方存储的集群名称, 接口拿到的数据',
  table_name varchar(190) NOT NULL COMMENT 'PHBase方存储的 table ID, 接口拿到的数据',
  operator varchar(50) NOT NULL DEFAULT '' COMMENT '操作方',
  operation_group varchar(50) NOT NULL DEFAULT '' COMMENT '操作方组',
  start_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '查询开始时间',
  end_time datetime DEFAULT NULL COMMENT '查询结束时间',
  query_time decimal(11,5) NOT NULL DEFAULT '0.00000' COMMENT '查询时间(ms)',
  status tinyint(4) NOT NULL DEFAULT '1' COMMENT '0:None, 1:查询中, 2:查询成功, 3: 查询失败',
  count bigint(20) NOT NULL DEFAULT '0' COMMENT '获取行数',
  statement longtext COMMENT '查询语句',
  message text COMMENT '其他信息',
  extra text COMMENT '预留字段，用作扩展,json格式的字符串',
  created_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  updated_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (id),
  KEY idx_username_cluster_table (username,cluster_name,table_name),
  KEY idx_cluster_table (cluster_name,table_name),
  KEY idx_business (business),
  KEY idx_start_end_time (start_time,end_time),
  KEY idx_end_time (end_time),
  KEY idx_operator (operator),
  KEY idx_operation_group (operation_group),
  KEY idx_created_at (created_at),
  KEY idx_updated_at (updated_at)
) ENGINE=InnoDB AUTO_INCREMENT=32 DEFAULT CHARSET=utf8mb4 COMMENT='phbase查询日志'
PARTITION BY LIST COLUMNS(store_id, id) (
    PARTITION pNorth VALUES IN ((3, 2),(5, 2),(6, 2),(9, 1),(17,1)),
    PARTITION pEast VALUES IN ((3, 2),(5, 2),(6, 2),(17,1)),
    PARTITION pWest VALUES IN ((3, 2),(5, 2),(6, 2),(9, 1),(17,1)),
    PARTITION pCentral VALUES IN ((3, 2),(5, 2),(6, 2))
)
`
	ps := New()
	stmt, err := ps.ParseOneStmt(query, "", "")
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(stmt.Text())

	var sb1 strings.Builder
	if err = stmt.Restore(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb1)); err != nil {
		t.Fatalf("Restore 出错. %s", err.Error())
	}
	fmt.Println("Restore 语句:", sb1.String())

	var sb2 strings.Builder
	if err = stmt.Pretty(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb2), 1, 4, " "); err != nil {
		t.Fatalf("Pretty 出错. %s", err.Error())
	}

	fmt.Println("Pretty 语句:", sb2.String())
}

func Test_Pretty_CreateTable_SubPartition(t *testing.T) {
	query := `
CREATE TABLE es_query_logs (
  updated_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB AUTO_INCREMENT=32 DEFAULT CHARSET=utf8mb4 COMMENT='phbase查询日志'
PARTITION BY RANGE COLUMNS(create_time)
SUBPARTITION BY HASH( HOUR(create_time) )
    SUBPARTITIONS 24 (
        PARTITION p20190316 VALUES LESS THAN ('2019-03-16'),
        PARTITION p20190316 VALUES LESS THAN ('2019-03-17'),
        PARTITION p20190317 VALUES LESS THAN ('2019-03-18'),
        PARTITION p20190318 VALUES LESS THAN ('2019-03-19')
    );
`
	ps := New()
	stmt, err := ps.ParseOneStmt(query, "", "")
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(stmt.Text())

	var sb1 strings.Builder
	if err = stmt.Restore(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb1)); err != nil {
		t.Fatalf("Restore 出错. %s", err.Error())
	}
	fmt.Println("Restore 语句:", sb1.String())

	var sb2 strings.Builder
	if err = stmt.Pretty(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb2), 1, 4, " "); err != nil {
		t.Fatalf("Pretty 出错. %s", err.Error())
	}

	fmt.Println("Pretty 语句:", sb2.String())
}

func Test_Pretty_CreateTable_SubPartition_02(t *testing.T) {
	query := `
CREATE TABLE ts (id INT, purchased DATE)
    PARTITION BY RANGE(YEAR(purchased))
    SUBPARTITION BY HASH(TO_DAYS(purchased))
    (
        PARTITION p0 VALUES LESS THAN (1990)
        (
            SUBPARTITION s0,
            SUBPARTITION s1
        ),
        PARTITION p1 VALUES LESS THAN (2000)
        (
            SUBPARTITION s2,
            SUBPARTITION s3
        ),
        PARTITION p2 VALUES LESS THAN MAXVALUE
        (
            SUBPARTITION s4,
            SUBPARTITION s5
        )
    );
`
	ps := New()
	stmt, err := ps.ParseOneStmt(query, "", "")
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(stmt.Text())

	var sb1 strings.Builder
	if err = stmt.Restore(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb1)); err != nil {
		t.Fatalf("Restore 出错. %s", err.Error())
	}
	fmt.Println("Restore 语句:", sb1.String())

	var sb2 strings.Builder
	if err = stmt.Pretty(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb2), 1, 4, " "); err != nil {
		t.Fatalf("Pretty 出错. %s", err.Error())
	}

	fmt.Println("Pretty 语句:", sb2.String())
}

func Test_Pretty_RenameTables_01(t *testing.T) {
	query := `rename table t1 to t_1, t2 to t_2, t3 to t_3`
	ps := New()
	stmt, err := ps.ParseOneStmt(query, "", "")
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(stmt.Text())

	var sb1 strings.Builder
	if err = stmt.Restore(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb1)); err != nil {
		t.Fatalf("Restore 出错. %s", err.Error())
	}
	fmt.Println("Restore 语句:", sb1.String())

	var sb2 strings.Builder
	if err = stmt.Pretty(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb2), 1, 4, " "); err != nil {
		t.Fatalf("Pretty 出错. %s", err.Error())
	}

	fmt.Println("Pretty 语句:", sb2.String())
}

func Test_Pretty_CreateView_01(t *testing.T) {
	query := `CREATE VIEW v_1(name_1, name_2, name_3, name_4, name_5, name_6, name_7, name_8, name_9, name_10) AS select id, name, name_1 from t_1`
	ps := New()
	stmt, err := ps.ParseOneStmt(query, "", "")
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(stmt.Text())

	var sb1 strings.Builder
	if err = stmt.Restore(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb1)); err != nil {
		t.Fatalf("Restore 出错. %s", err.Error())
	}
	fmt.Println("Restore 语句:", sb1.String())

	var sb2 strings.Builder
	if err = stmt.Pretty(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb2), 1, 4, " "); err != nil {
		t.Fatalf("Pretty 出错. %s", err.Error())
	}

	fmt.Println("Pretty 语句:", sb2.String())
}

func Test_Pretty_AlterTable_01(t *testing.T) {
	query := `
ALTER TABLE t1
    ADD COLUMN (
        id int,
        name int
    ),
    ADD COLUMN name_1 int,
    ADD INDEX idx_name(iname),
    ADD INDEX idx_id_name(id, name),
    DROP COLUMN a,
    DROP INDEX idx_id,
    add partition (
        Partition p3 values less than(maxvalue),
        Partition p5 values less than(maxvalue)
    ),
    reorganize partition p0 into (
        partition n0 values less than(5000),
        partition n1 values less than(10000)
    ),
    drop partition p1,p2
`
	ps := New()
	stmt, err := ps.ParseOneStmt(query, "", "")
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(stmt.Text())

	var sb1 strings.Builder
	if err = stmt.Restore(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb1)); err != nil {
		t.Fatalf("Restore 出错. %s", err.Error())
	}
	fmt.Println("Restore 语句:", sb1.String())

	var sb2 strings.Builder
	if err = stmt.Pretty(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb2), 1, 4, " "); err != nil {
		t.Fatalf("Pretty 出错. %s", err.Error())
	}

	fmt.Println("Pretty 语句:")
	fmt.Println(sb2.String())
}

func Test_Pretty_Select_01(t *testing.T) {
	query := `
SELECT DISTINCT id1, id2, id3, (
        SELECT * FROM t1 WHERE id = 1
    ), id5, id6, id7
FROM (
	SELECT * FROM (
        SELECT * FROM t2
        LEFT JOIN t1
            ON t2.id = t1.id
        WHERE id = 1
            AND id IN(1,2,4,5,6,7)
            AND (time BETWEEN 'A' AND 'Z'
            OR time BETWEEN 'a' AND 'z')
            AND id = 1
            AND id IN(
                SELECT * FROM t1 where name = 'a'
            )
        GROUP BY id
        ORDER BY id DESC, name ASC
        LIMIT 100
    ) AS tmp2
) as tmp1
LEFT JOIN t2
    ON t1.a = t2.a
LEFT JOIN t3
    ON t2.a = t3.a
LEFT JOIN t4
    ON t3.a = t4.a
WHERE id = 1
GROUP BY id
ORDER BY id DESC, name ASC
LIMIT 1,100
`
	ps := New()
	stmt, err := ps.ParseOneStmt(query, "", "")
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(stmt.Text())

	var sb1 strings.Builder
	if err = stmt.Restore(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb1)); err != nil {
		t.Fatalf("Restore 出错. %s", err.Error())
	}
	fmt.Println("Restore 语句:", sb1.String())

	var sb2 strings.Builder
	if err = stmt.Pretty(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb2), 0, 4, " "); err != nil {
		t.Fatalf("Pretty 出错. %s", err.Error())
	}

	fmt.Println("Pretty 语句:")
	fmt.Println(sb2.String())
}
func Test_Pretty_Select_Union_01(t *testing.T) {
	query := `
SELECT DISTINCT id1, id2, id3, (
        SELECT * FROM t1 WHERE id = 1
    ), id5, id6, id7
FROM (
	SELECT * FROM (
        SELECT * FROM t2
        LEFT JOIN t1
            ON t2.id = t1.id
        WHERE id = 1
            AND id IN(1,2,4,5,6,7)
            AND (time BETWEEN 'A' AND 'Z'
            OR time BETWEEN 'a' AND 'z')
            AND id = 1
            AND id IN(
                SELECT * FROM t1 where name = 'a'
            )
        GROUP BY id
        ORDER BY id DESC, name ASC
        LIMIT 100
    ) AS tmp2
) as tmp1
LEFT JOIN t2
    ON t1.a = t2.a
LEFT JOIN t3
    ON t2.a = t3.a
LEFT JOIN t4
    ON t3.a = t4.a
WHERE id = 1
GROUP BY id
ORDER BY id DESC, name ASC
LIMIT 1,100
UNION ALL
SELECT DISTINCT id1, id2, id3, (
        SELECT * FROM t1 WHERE id = 1
    ), id5, id6, id7
FROM (
	SELECT * FROM (
        SELECT * FROM t2
        LEFT JOIN t1
            ON t2.id = t1.id
        WHERE id = 1
            AND id IN(1,2,4,5,6,7)
            AND (time BETWEEN 'A' AND 'Z'
            OR time BETWEEN 'a' AND 'z')
            AND id = 1
            AND id IN(
                SELECT * FROM t1 where name = 'a'
            )
        GROUP BY id
        ORDER BY id DESC, name ASC
        LIMIT 100
    ) AS tmp2
) as tmp1
LEFT JOIN t2
    ON t1.a = t2.a
LEFT JOIN t3
    ON t2.a = t3.a
LEFT JOIN t4
    ON t3.a = t4.a
WHERE id = 1
GROUP BY id
ORDER BY id DESC, name ASC
LIMIT 1,100
`
	ps := New()
	stmt, err := ps.ParseOneStmt(query, "", "")
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(stmt.Text())

	var sb1 strings.Builder
	if err = stmt.Restore(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb1)); err != nil {
		t.Fatalf("Restore 出错. %s", err.Error())
	}
	fmt.Println("Restore 语句:", sb1.String())

	var sb2 strings.Builder
	if err = stmt.Pretty(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb2), 0, 4, " "); err != nil {
		t.Fatalf("Pretty 出错. %s", err.Error())
	}

	fmt.Println("Pretty 语句:")
	fmt.Println(sb2.String())
}

func Test_Pretty_InsertIntoValues(t *testing.T) {
	query := `
INSERT INTO t1 VALUES(1, 2),(1),(1),(1),(1),(1),(1);
`
	ps := New()
	stmt, err := ps.ParseOneStmt(query, "", "")
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(stmt.Text())

	var sb1 strings.Builder
	if err = stmt.Restore(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb1)); err != nil {
		t.Fatalf("Restore 出错. %s", err.Error())
	}
	fmt.Println("Restore 语句:", sb1.String())

	var sb2 strings.Builder
	if err = stmt.Pretty(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb2), 0, 4, " "); err != nil {
		t.Fatalf("Pretty 出错. %s", err.Error())
	}

	fmt.Println("Pretty 语句:")
	fmt.Println(sb2.String())
}

func Test_Pretty_InsertSelect(t *testing.T) {
	query := `
INSERT INTO t1 SELECT * FROM t
`
	ps := New()
	stmt, err := ps.ParseOneStmt(query, "", "")
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(stmt.Text())

	var sb1 strings.Builder
	if err = stmt.Restore(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb1)); err != nil {
		t.Fatalf("Restore 出错. %s", err.Error())
	}
	fmt.Println("Restore 语句:", sb1.String())

	var sb2 strings.Builder
	if err = stmt.Pretty(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb2), 0, 4, " "); err != nil {
		t.Fatalf("Pretty 出错. %s", err.Error())
	}

	fmt.Println("Pretty 语句:")
	fmt.Println(sb2.String())
}

func Test_Pretty_InsertSet(t *testing.T) {
	query := `
insert into t set field1 = 'value1',field2 = 'value2'
`
	ps := New()
	stmt, err := ps.ParseOneStmt(query, "", "")
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(stmt.Text())

	var sb1 strings.Builder
	if err = stmt.Restore(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb1)); err != nil {
		t.Fatalf("Restore 出错. %s", err.Error())
	}
	fmt.Println("Restore 语句:", sb1.String())

	var sb2 strings.Builder
	if err = stmt.Pretty(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb2), 0, 4, " "); err != nil {
		t.Fatalf("Pretty 出错. %s", err.Error())
	}

	fmt.Println("Pretty 语句:")
	fmt.Println(sb2.String())
}

func Test_Pretty_Delete(t *testing.T) {
	query := `
DELETE t1, t2 FROM t
LEFT JOIN t1
    ON t.a = t1.a
WHERE id = 1
`
	ps := New()
	stmt, err := ps.ParseOneStmt(query, "", "")
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(stmt.Text())

	var sb1 strings.Builder
	if err = stmt.Restore(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb1)); err != nil {
		t.Fatalf("Restore 出错. %s", err.Error())
	}
	fmt.Println("Restore 语句:", sb1.String())

	var sb2 strings.Builder
	if err = stmt.Pretty(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb2), 0, 4, " "); err != nil {
		t.Fatalf("Pretty 出错. %s", err.Error())
	}

	fmt.Println("Pretty 语句:")
	fmt.Println(sb2.String())
}

func Test_Pretty_Update(t *testing.T) {
	query := `
UPDATE t1
LEFT JOIN t2
    ON t1.a = t2.a
SET a = 1,
    b = 2
WHERE id = 1
`
	ps := New()
	stmt, err := ps.ParseOneStmt(query, "", "")
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(stmt.Text())

	var sb1 strings.Builder
	if err = stmt.Restore(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb1)); err != nil {
		t.Fatalf("Restore 出错. %s", err.Error())
	}
	fmt.Println("Restore 语句:", sb1.String())

	var sb2 strings.Builder
	if err = stmt.Pretty(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb2), 0, 4, " "); err != nil {
		t.Fatalf("Pretty 出错. %s", err.Error())
	}

	fmt.Println("Pretty 语句:")
	fmt.Println(sb2.String())
}

func Test_Pretty_GroupConcat(t *testing.T) {
	query := `
SELECT GROUP_CONCAT(id, ":"), GROUP_CONCAT(id) FROM t
`
	ps := New()
	stmt, err := ps.ParseOneStmt(query, "", "")
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(stmt.Text())

	var sb1 strings.Builder
	if err = stmt.Restore(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb1)); err != nil {
		t.Fatalf("Restore 出错. %s", err.Error())
	}
	fmt.Println("Restore 语句:", sb1.String())

	var sb2 strings.Builder
	if err = stmt.Pretty(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb2), 0, 4, " "); err != nil {
		t.Fatalf("Pretty 出错. %s", err.Error())
	}

	fmt.Println("Pretty 语句:")
	fmt.Println(sb2.String())
}

func Test_Pretty_GroupConcat_02(t *testing.T) {
	query := `
SELECT GROUP_CONCAT(DISTINCT v ORDER BY v ASC SEPARATOR ';') FROM t
`
	ps := New()
	stmt, err := ps.ParseOneStmt(query, "", "")
	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println(stmt.Text())

	var sb1 strings.Builder
	if err = stmt.Restore(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb1)); err != nil {
		t.Fatalf("Restore 出错. %s", err.Error())
	}
	fmt.Println("Restore 语句:", sb1.String())

	var sb2 strings.Builder
	if err = stmt.Pretty(format1.NewRestoreCtx(format1.DefaultRestoreFlags, &sb2), 0, 4, " "); err != nil {
		t.Fatalf("Pretty 出错. %s", err.Error())
	}

	fmt.Println("Pretty 语句:")
	fmt.Println(sb2.String())
}
