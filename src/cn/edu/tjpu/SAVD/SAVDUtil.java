package cn.edu.tjpu.SAVD;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;


public class SAVDUtil {

	public static final int DP = 4; // 定义聚合度
	public static final int VD_DP = 4; // 定义垂直分区聚合度
	public static final int K = 4; // 定义分区数
	public static final double w = 0.6; // 定义取值范围
	public static double MAX = 0;   //定义map1中找出的最大值
	public static double MIN = 10000;   //定义map1中找出的最小值
	public static Map<String, List<String>> map1 = new HashMap<String, List<String>>();
	public static Map<Integer, List<Double>> map2 = new HashMap<Integer, List<Double>>();
	/**
	 * 计算欧几里得距离
	 * 
	 * @param list1
	 * @param list2
	 * @return
	 */
	
	
	public static double euclidean_distance(List<Double> list1, List<Double> list2) {
		double distance = 0;

		if (list1.size() == list2.size()) {
			for (int i = 0; i < list1.size(); i++) {
				double temp = Math.pow((list1.get(i) - list2.get(i)), 2);
				distance += temp;
			}
			distance = Math.sqrt(distance);
		}
		return distance;
	}

	/**
	 * 计算PAA距离
	 * 
	 * @param list1
	 * @param list2
	 * @return
	 */
	public static double paa_distance(List<Double> list1, List<Double> list2) {
		double distance = 0;

		if (list1.size() == list2.size()) {
			for (int i = 0; i < list1.size(); i++) {
				double temp = Math.pow(list1.get(i) - list2.get(i), 2) * DP;
				distance += temp;
			}
			distance = Math.sqrt(distance);
		}
		return distance;
	}

	/**
	 * 判断两个SAX是否相同
	 * 
	 * @param list1
	 * @param list2
	 * @return
	 */
	public static boolean sax_equal(List<String> list1, List<String> list2) {
		if (list1.size() == list2.size()) {
			for (int i = 0; i < list1.size(); i++) {
				if (!list1.get(i).equals(list2.get(i))) {
					return false;
				}
			}
		}

		return true;

	}

	/**
	 * 计算SAX
	 * 
	 * @param paa
	 * @return
	 */
	public static String getSax(double paa) {
		String sa = "A0";
		if (paa >= 0 && paa < 0.1)
			sa = "A1";
		else if (paa >= 0.1 && paa < 0.2)
			sa = "A2";
		else if (paa >= 0.2 && paa < 0.3)
			sa = "A3";
		else if (paa >= 0.3 && paa < 0.4)
			sa = "A4";
		else if (paa >= 0.4 && paa < 0.5)
			sa = "A5";
		else if (paa >= 0.5 && paa < 0.6)
			sa = "A6";
		else if (paa >= 0.6 && paa < 0.7)
			sa = "A7";
		else if (paa >= 0.7 && paa < 0.8)
			sa = "A8";
		else if (paa >= 0.8 && paa < 0.9)
			sa = "A9";
		else if (paa >= 0.9 && paa < 1.0)
			sa = "A10";
		return sa;
	}
	/**
	 * 通过SAX获取到近似数据值
	 * @param sax
	 * @return
	 */
	public static double getValueBySax(String sax) {
		double value;
		switch (sax) {
			case "A1": value = 0.05; break;
			case "A2": value = 0.15; break;
			case "A3": value = 0.25; break;
			case "A4": value = 0.35; break;
			case "A5": value = 0.45; break;
			case "A6": value = 0.55; break;
			case "A7": value = 0.65; break;
			case "A8": value = 0.75; break;
			case "A9": value = 0.85; break;
			case "A10": value = 0.95; break;
			default: value = 0; break;
		}		
		return value;
	}
	/**
	 * 计算直方图交集中的sax距离
	 * @param arr1
	 * @param arr2
	 * @return
	 */
	public static double histogram_intersection_sax_distance(List<String> arr1, List<String> arr2){
	    double  distance = 0;
	    if (arr1.size() == arr2.size()) {
	      for (int i=0;i<arr1.size();i++) {
	        distance += Math.min(getValueBySax(arr1.get(i)),getValueBySax(arr2.get(i)));
	      }
	    }
	    return distance;
	}
	/**
	 * 计算SAX距离
	 * 
	 * @param list1
	 * @param list2
	 * @return
	 */
	public static double sax_distance(List<String> list1, List<String> list2) {
		double distance = 0;

		if (list1.size() == list2.size()) {
			for (int i = 0; i < list1.size(); i++) {
				double temp = Math.pow(list1.get(i).compareTo(list2.get(i)) * 0.25, 2);
				distance += temp;
			}
			distance = Math.sqrt(distance * DP);
		}
		return distance;
	}
	
	public static double histogram_intersection_sax_distance(String[] s1, String[] s2) {
		double distance = 0;

		if (s1.length == s2.length) {
			for (int i = 0; i < s1.length; i++) {
				double temp = Math.min(getValueBySax(s1[i]),getValueBySax(s2[i]));
				distance += temp;
			}
		}
		return distance;
	}
	
	public static double histogram_intersection_distance(List<Double> l1, List<Double> l2) {
		double distance = 0;

		if (l1.size() == l2.size()) {
			for (int i = 0; i < l1.size(); i++) {
				double temp = Math.min(l1.get(i),l2.get(i));
				distance += temp;
			}
		}
		return distance;
	}

	/**
	 * 判断路径是否存在
	 *
	 * @param conf
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static boolean exits(Configuration conf, String path) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		return fs.exists(new Path(path));
	}

	/**
	 * 
	 *
	 * @param conf
	 * @param filePath
	 * @param contents
	 * @throws IOException
	 */
	public static void createFile(Configuration conf, String filePath, byte[] contents) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(filePath);
		FSDataOutputStream outputStream = fs.create(path);
		outputStream.write(contents);
		outputStream.close();
		fs.close();
	}

	/**
	 * 
	 *
	 * @param conf
	 * @param filePath
	 * @param fileContent
	 * @throws IOException
	 */
	public static void createFile(Configuration conf, String filePath, String fileContent) throws IOException {
		createFile(conf, filePath, fileContent.getBytes());
	}

	/**
	 * @param conf
	 * @param localFilePath
	 * @param remoteFilePath
	 * @throws IOException
	 */
	public static void copyFromLocalFile(Configuration conf, String localFilePath, String remoteFilePath) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path localPath = new Path(localFilePath);
		Path remotePath = new Path(remoteFilePath);
		fs.copyFromLocalFile(true, true, localPath, remotePath);
		fs.close();
	}

	/**
	 * 删除目录或文件
	 *
	 * @param conf
	 * @param remoteFilePath
	 * @param recursive
	 * @return
	 * @throws IOException
	 */
	public static boolean deleteFile(Configuration conf, String remoteFilePath, boolean recursive) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		boolean result = fs.delete(new Path(remoteFilePath), recursive);
		fs.close();
		return result;
	}

	/**
	 * 删除目录或文件(如果有子目录,则级联删除)
	 *
	 * @param conf
	 * @param remoteFilePath
	 * @return
	 * @throws IOException
	 */
	public static boolean deleteFile(Configuration conf, String remoteFilePath) throws IOException {
		return deleteFile(conf, remoteFilePath, true);
	}

	/**
	 * 文件重命名
	 *
	 * @param conf
	 * @param oldFileName
	 * @param newFileName
	 * @return
	 * @throws IOException
	 */
	public static boolean renameFile(Configuration conf, String oldFileName, String newFileName) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path oldPath = new Path(oldFileName);
		Path newPath = new Path(newFileName);
		boolean result = fs.rename(oldPath, newPath);
		fs.close();
		return result;
	}

	/**
	 * 创建目录
	 *
	 * @param conf
	 * @param dirName
	 * @return
	 * @throws IOException
	 */
	public static boolean createDirectory(Configuration conf, String dirName) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path dir = new Path(dirName);
		boolean result = fs.mkdirs(dir);
		fs.close();
		return result;
	}

	/**
	 * 列出指定路径下的所有文件(不包含目录)
	 *
	 * @param conf
	 * @param basePath
	 * @param recursive
	 */
	public static RemoteIterator<LocatedFileStatus> listFiles(FileSystem fs, String basePath, boolean recursive) throws IOException {

		RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = fs.listFiles(new Path(basePath), recursive);

		return fileStatusRemoteIterator;
	}

	/**
	 * 列出指定路径下的文件（非递归）
	 *
	 * @param conf
	 * @param basePath
	 * @return
	 * @throws IOException
	 */
	public static RemoteIterator<LocatedFileStatus> listFiles(Configuration conf, String basePath) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path(basePath), false);
		fs.close();
		return remoteIterator;
	}

	/**
	 * 列出指定目录下的文件\子目录信息（非递归）
	 *
	 * @param conf
	 * @param dirPath
	 * @return
	 * @throws IOException
	 */
	public static FileStatus[] listStatus(Configuration conf, String dirPath) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fileStatuses = fs.listStatus(new Path(dirPath));
		fs.close();
		return fileStatuses;
	}

	/**
	 * 读取文件内容
	 *
	 * @param conf
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	public static String readFile(Configuration conf, String filePath) throws IOException {
		String fileContent = null;
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(filePath);
		InputStream inputStream = null;
		ByteArrayOutputStream outputStream = null;
		try {
			inputStream = fs.open(path);
			outputStream = new ByteArrayOutputStream(inputStream.available());
			IOUtils.copyBytes(inputStream, outputStream, conf);
			fileContent = outputStream.toString();
		} finally {
			IOUtils.closeStream(inputStream);
			IOUtils.closeStream(outputStream);
			fs.close();
		}
		return fileContent;
	}

	/**
	 * 测试程序
	 * 
	 * @param args
	 */
//	public static void main(String[] args) {
//		List<String> l1 = new ArrayList<String>();
//		l1.add("A1");
//		l1.add("A2");
//		l1.add("A3");
//		List<String> l2 = new ArrayList<String>();
//		l2.add("A1");
//		l2.add("A3");
//		l2.add("A2");
//		List<Double> l3 = new ArrayList<Double>();
//		l3.add(0.024193548);
//		l3.add(0.38709676);
//		l3.add(0.5);
//		List<Double> l4 = new ArrayList<Double>();
//		l4.add(0.01724138);
//		l4.add(0.6896552);
//		l4.add(0.46551725);
//		double s = sax_distance(l1, l2);
//		double paa_distance = paa_distance(l3, l4);
//		System.out.println(s);
//		System.out.println(paa_distance);
//	}

}
