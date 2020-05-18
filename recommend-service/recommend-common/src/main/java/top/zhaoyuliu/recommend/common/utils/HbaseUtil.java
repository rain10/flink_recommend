package top.zhaoyuliu.recommend.common.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseUtil {

	private static Logger log = LoggerFactory.getLogger(HbaseUtil.class);

	protected static Configuration conf = null;
	protected static Connection connection = null;
	protected static Admin admin = null;

	/**
	 * @desc 取得连接
	 */
	public static void getHbaseConnection() {
		try {
			log.info("创建hbase配置文件 ->{} ->{}", ExecutionEnvUtil.PARAMETER_TOOL.get("hbase.zookeeper.quorum"),
					ExecutionEnvUtil.PARAMETER_TOOL.get("hbase.zookeeper.property.clientPort"));
			conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", ExecutionEnvUtil.PARAMETER_TOOL.get("hbase.zookeeper.quorum"));// zookeeper地址
			conf.set("hbase.zookeeper.property.clientPort",
					ExecutionEnvUtil.PARAMETER_TOOL.get("hbase.zookeeper.property.clientPort"));
			connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();
			log.info("hbase admin -> {}",admin);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @desc 连关闭接
	 */
	public static void close() {
		try {
			if (connection != null) {
				connection.close();
			}

			if (admin != null) {
				admin.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @desc 创建表
	 */
	public static void createTable(String nameSpace, String tableName, String family) {
		try {
			// 创建表空间
			// NamespaceDescriptor descriptor=NamespaceDescriptor.create(nameSpace).build();
			// admin.createNamespace(descriptor);

			TableName tName = TableName.valueOf(nameSpace, tableName);

			// 如果表存在，删除表
			if (admin.tableExists(tName)) {
				admin.disableTable(tName);
				admin.deleteTable(tName);
			} else {

				HTableDescriptor tableDesc = new HTableDescriptor(tName);

				HColumnDescriptor colDesc = new HColumnDescriptor(family.getBytes());
				colDesc.setMaxVersions(1);// 1个版本
				colDesc.setInMemory(true);// 开启内存缓存

				tableDesc.addFamily(colDesc);
				admin.createTable(tableDesc);// 直接创建表
				// admin.createTable(tableDesc, splitKeys);//创建表，添加预分区，避免热点写,若不指定splitKeys为空即可
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void createTable1(String tableName, String family) {
		try {

			TableName tName = TableName.valueOf(tableName);

			// 如果表存在，删除表
			if (admin.tableExists(tName)) {
				admin.disableTable(tName);
				admin.deleteTable(tName);
			} else {

				HTableDescriptor tableDesc = new HTableDescriptor(tName);

				HColumnDescriptor colDesc = new HColumnDescriptor(family.getBytes());
				colDesc.setMaxVersions(1);// 1个版本
				colDesc.setInMemory(true);// 开启内存缓存

				tableDesc.addFamily(colDesc);
				admin.createTable(tableDesc);// 直接创建表
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void createTable2(String tableName, String family) {
		try {

			TableName tName = TableName.valueOf(tableName);

			// 如果表不存在
			if (!admin.tableExists(tName)) {
				HTableDescriptor tableDesc = new HTableDescriptor(tName);

				HColumnDescriptor colDesc = new HColumnDescriptor(family.getBytes());
				colDesc.setMaxVersions(1);// 1个版本
				colDesc.setInMemory(true);// 开启内存缓存

				tableDesc.addFamily(colDesc);
				admin.createTable(tableDesc);// 直接创建表
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @description 删除row列族下的某列值
	 */
	public static void deleteQualifierValue(String tableName, String rowKey, String family, String qualifier) {

		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
			Delete delete = new Delete(rowKey.getBytes());
			delete.addColumn(family.getBytes(), qualifier.getBytes());
			table.delete(delete);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * @desc 删除一行
	 */
	public static void deleteRow(String tableName, String rowKey, String family) {

		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
			Delete delete = new Delete(rowKey.getBytes());
			delete.addFamily(family.getBytes());
			table.delete(delete);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * @desc 插入一条记录
	 */
	public static void addOneRecord(String tableName, String rowKey, String family, String qualifier, String value) {
		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));

			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 添加多条记录
	 */
	public static void addMoreRecord(String tableName, String family, String qualifier, List<String> rowList,
			String value) {
		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));

			List<Put> puts = new ArrayList<>();
			Put put = null;
			for (int i = 0; i < rowList.size(); i++) {
				put = new Put(Bytes.toBytes(rowList.get(i)));
				put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));

				puts.add(put);
			}
			table.put(puts);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

	/**
	 * @desc 查询rowkey下某一列值
	 */
	public static String getValue(String tableName, String rowKey, String family, String qualifier) {
		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));

			Get get = new Get(rowKey.getBytes());
			get.addColumn(family.getBytes(), qualifier.getBytes());// 返回指定列族、列名，避免rowKey下所有数据

			Result rs = table.get(get);
			Cell cell = rs.getColumnLatestCell(family.getBytes(), qualifier.getBytes());

			String value = null;
			if (cell != null) {
				value = Bytes.toString(CellUtil.cloneValue(cell));
			}

			return value;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}

	/**
	 * @desc 获取一行数据
	 */
	public static List<Cell> getRowCells(String tableName, String rowKey, String family) {
		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
			Get get = new Get(rowKey.getBytes());
			get.addFamily(family.getBytes());

			Result rs = table.get(get);

			List<Cell> cellList = rs.listCells();
			// 如果需要,遍历cellList
			// if (cellList!=null) {
			// String qualifier = null;
			// String value = null;
			// for (Cell cell : cellList) {
			// qualifier = Bytes.toString(
			// cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
			// value = Bytes.toString( cell.getValueArray(), cell.getValueOffset(),
			// cell.getValueLength());
			// System.out.println(qualifier+"--"+value);
			// }
			// }
			return cellList;
		} catch (IOException e) {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		}
		return null;
	}

	/**
	 * 全表扫描
	 * 
	 * @param tableName
	 * @return
	 */
	public static ResultScanner scan(String tableName, String family, String qualifier) {
		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));

			Scan scan = new Scan();
			ResultScanner rs = table.getScanner(scan);
			// 一般返回ResultScanner，遍历即可
			// if (rs!=null){
			// String row = null;
			// String quali = null;
			// String value = null;
			// for (Result result : rs) {
			// row =
			// Bytes.toString(CellUtil.cloneRow(result.getColumnLatestCell(family.getBytes(),
			// qualifier.getBytes())));
			// quali
			// =Bytes.toString(CellUtil.cloneQualifier(result.getColumnLatestCell(family.getBytes(),
			// qualifier.getBytes())));
			// value
			// =Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(family.getBytes(),
			// qualifier.getBytes())));
			// System.out.println(row+"-"+quali+"-"+value);
			// }
			// }
			return rs;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}

	/**
	 * @desc hbase简单分页查询
	 * @param tableName
	 * @param endRow
	 * @param pageSize
	 */
	public static List<Result> pageFilter(String tableName, String endRow, int pageSize) {

		ResultScanner scanner = null;
		Table table = null;
		List<Result> list = new ArrayList<>();
		try {
			table = connection.getTable(TableName.valueOf(tableName));

			byte[] POSTFIX = new byte[0x00];// 长度为零的字节数组，0x00十六进制表示0

			Filter filter = new PageFilter(pageSize);
			Scan scan = new Scan();
			scan.setFilter(filter);

			// 每次查询的最后一条记录endRow作为新的startRow
			if (endRow != null) {// 这里为啥加POSTFIX不是很明白，好像是为了区分下一页，但是我去掉结果也没影响
				byte[] startRow = Bytes.add(Bytes.toBytes(endRow), POSTFIX);
				scan.withStartRow(startRow);
			}
			scanner = table.getScanner(scan);
			Result result;
			while ((result = scanner.next()) != null) {
				list.add(result);
			}
			return list;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (scanner != null) {
				scanner.close();
			}
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}

	public static void main(String[] args) throws IOException {
		getHbaseConnection();

		String t_task_log = "t_task_other_mac";
		String endRow = null;// 开始row
		int pageNum = 1;
		int PAGESIZE = 200;// 每页须大于1

		List<Result> resultList = null;
		while (true) {
			// 分页查询t_task_other_mac表
			resultList = pageFilter(t_task_log, endRow, PAGESIZE);

			int size = resultList.size();
			if (size > 1) {
				Result rr = resultList.get(size - 1);
				endRow = Bytes.toString(rr.getRow());
				// 如果本页等于pagesize,不是最后一页，移除stopRow,stopRow不包含在本次处理，
				// 如果本页小于pagesize表明是最后一页，不再移除stopRow,stopRow包含本次处理
				if (size == PAGESIZE) {
					resultList.remove(rr);
				}
			}
			System.out.println("=========第" + pageNum + "页===========" + size + "===");
			// 遍历Result,进行自己的业务处理
			for (Result r : resultList) {
				System.out.println(Bytes.toString(r.getRow()));
				// String row =
				// Bytes.toString(CellUtil.cloneRow(result.getColumnLatestCell("family".getBytes(),
				// "qualifier".getBytes())));
				// String quali
				// =Bytes.toString(CellUtil.cloneQualifier(result.getColumnLatestCell("family".getBytes(),
				// "qualifier".getBytes())));
				// String value
				// =Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell("family".getBytes(),
				// "qualifier".getBytes())));
			}

			// 当每页返回size小于pagesize停止，结束循环
			if (size < PAGESIZE) {
				break;
			}
			pageNum++;
		}

		close();
	}
}
