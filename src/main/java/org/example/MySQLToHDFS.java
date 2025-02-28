package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class MySQLToHDFS {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

// 获取HDFS⽂件系统对象
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://8.146.207.218:9000");
        conf.set("dfs.client.use.datanode.hostname", "true");
        FileSystem hdfs = FileSystem.get(conf);
// 创建要操作的⽂件对象

        Path path = new Path("F:\\nvv2t_md.csv");
        Path dst = new Path("/charger_data_input/nvv2t_md.csv");
// 上传⽂件
        hdfs.copyFromLocalFile(path, dst);
        System.out.println(dst);
        System.out.println("⽂件上传完成");
    }
}
