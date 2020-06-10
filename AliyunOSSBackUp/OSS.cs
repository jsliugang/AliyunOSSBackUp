using Aliyun.OSS;
using Aliyun.OSS.Common;
using Aliyun.OSS.Util;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.IO.Compression;
using System.Linq;

namespace AliyunOSSBackUp
{

    public class OSS
    {
        public static readonly string AccessKeyId = ConfigurationManager.AppSettings["accessKeyId"];
        public static readonly string AccessKeySecret = ConfigurationManager.AppSettings["accessKeySecret"];
        public static readonly string Endpoint = ConfigurationManager.AppSettings["endpoint"];
        public static readonly string BackupFilePath = ConfigurationManager.AppSettings["backupFilePath"];
        public static readonly string BucketName = ConfigurationManager.AppSettings["bucketName"];
        public static readonly string BaseObjectName = ConfigurationManager.AppSettings["baseobjectName"];
        public static readonly int MaxSizeRollBackups = Convert.ToInt32(ConfigurationManager.AppSettings["maxSizeRollBackups"]);
        private static string directoryPath = @"c:\temp";
        
        

        public static void Execut()
        {
            foreach (var item in BackupFilePath.Split(';'))
            {
                Console.WriteLine($"Begain backup  {item}......");
                var localFilename = BackUp(item);
                Console.WriteLine($"Finish backup  {item}......");
                Console.WriteLine("Begain delete expired files......");
                var list = ListFile(item);
                DeleteFile(list);
                if (File.Exists(localFilename))
                    File.Delete(localFilename);
                Console.WriteLine("Finish Delete expired files......");
            }
            Console.WriteLine($"Backup finish ------{DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss.fff")}");
        }

        private static string BackUp(string backupFilePath)
        {
            if (!Directory.Exists(backupFilePath))
                Console.WriteLine("backupFilePath do not exists");
            var dir = new DirectoryInfo(backupFilePath);
            var zipName = $"{dir.Name}_{DateTime.Now.ToString("yyyyMMddHHmmss")}.zip";
            var localFilename = Path.Combine(directoryPath, zipName);
            if (!Directory.Exists(directoryPath))
                Directory.CreateDirectory(directoryPath);
            if (File.Exists(localFilename))
                File.Delete(localFilename);
            ZipFile.CreateFromDirectory(backupFilePath, localFilename);
            var objectName = $"{BaseObjectName.TrimEnd('/')}/{zipName}";
            var client = new OssClient(Endpoint, AccessKeyId, AccessKeySecret);
            try
            {
                UploadObjectRequest request = new UploadObjectRequest(BucketName, objectName, localFilename)
                {
                    PartSize = 8 * 1024 * 1024,
                    ParallelThreadCount = 3,
                    CheckpointDir = directoryPath,

                };
                client.ResumableUploadObject(request);
                Console.WriteLine("Resumable upload object:{0} succeeded", objectName);
            }
            catch (OssException ex)
            {
                Console.WriteLine("Failed with error code: {0}; Error info: {1}. \nRequestID:{2}\tHostID:{3}",
                    ex.ErrorCode, ex.Message, ex.RequestId, ex.HostId);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed with error info: {0}", ex.Message);
            }

            return localFilename;
        }

        private static List<string> ListFile(string backupFilePath)
        {
            var keys = new List<string>();
            var dir = new DirectoryInfo(backupFilePath);
            var prefix = $"{BaseObjectName.TrimEnd('/')}/{dir.Name}";
            var client = new OssClient(Endpoint, AccessKeyId, AccessKeySecret);
            try
            {
                ObjectListing result = null;
                string nextMarker = string.Empty;
                do
                {
                    var listObjectsRequest = new ListObjectsRequest(BucketName)
                    {
                        Marker = nextMarker,
                        MaxKeys = 200,
                        Prefix = prefix,
                    };
                    result = client.ListObjects(listObjectsRequest);
                    var list = result.ObjectSummaries.ToList().OrderByDescending(d => d.LastModified).Skip(MaxSizeRollBackups).ToList();
                    foreach (var summary in list)
                    {
                        Console.WriteLine(summary.Key);
                        keys.Add(summary.Key);
                    }
                    nextMarker = result.NextMarker;
                } while (result.IsTruncated);
                Console.WriteLine("List objects of bucket:{0} succeeded ", BucketName);
            }
            catch (OssException ex)
            {
                Console.WriteLine("Failed with error code: {0}; Error info: {1}. \nRequestID:{2}\tHostID:{3}",
                    ex.ErrorCode, ex.Message, ex.RequestId, ex.HostId);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed with error info: {0}", ex.Message);
            }
            return keys;
        }

        private static void DeleteFile(List<string> keys)
        {
            var client = new OssClient(Endpoint, AccessKeyId, AccessKeySecret);
            try
            {
                var quietMode = false;
                var request = new DeleteObjectsRequest(BucketName, keys, quietMode);
                var result = client.DeleteObjects(request);
                if ((result.Keys != null))
                {
                    foreach (var obj in result.Keys)
                    {
                        Console.WriteLine("Delete successfully : {0} ", obj.Key);
                    }
                }
                Console.WriteLine("Delete objects succeeded");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Delete objects failed. {0}", ex.Message);
            }
        }
    }
}
