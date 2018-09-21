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
        public static readonly string accessKeyId = ConfigurationManager.AppSettings["accessKeyId"];
        public static readonly string accessKeySecret = ConfigurationManager.AppSettings["accessKeySecret"];
        public static readonly string endpoint = ConfigurationManager.AppSettings["endpoint"];
        public static readonly string backupFilePath = ConfigurationManager.AppSettings["backupFilePath"];
        public static readonly string bucketName = ConfigurationManager.AppSettings["bucketName"];
        public static readonly string baseobjectName = ConfigurationManager.AppSettings["baseobjectName"];
        public static readonly int maxSizeRollBackups = Convert.ToInt32(ConfigurationManager.AppSettings["maxSizeRollBackups"]);
        private static string directoryPath = @"c:\temp";

        public static void Execut()
        {
            Console.WriteLine("Begain backup......");
            var localFilename = BackUp();
            Console.WriteLine("Finish backup......");
            Console.WriteLine("Begain Delete expired files......");
            var list = ListFile();
            DeleteFile(list);
            if (File.Exists(localFilename))
                File.Delete(localFilename);
            Console.WriteLine("Finish Delete expired files......");
        }

        private static string BackUp()
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
            var objectName = $"{baseobjectName.TrimEnd('/')}/{zipName}";
            var client = new OssClient(endpoint, accessKeyId, accessKeySecret);
            try
            {
                UploadObjectRequest request = new UploadObjectRequest(bucketName, objectName, localFilename)
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

        private static List<string> ListFile()
        {
            var keys = new List<string>();
            var dir = new DirectoryInfo(backupFilePath);
            var prefix = $"{baseobjectName.TrimEnd('/')}/{dir.Name}";
            var client = new OssClient(endpoint, accessKeyId, accessKeySecret);
            try
            {
                ObjectListing result = null;
                string nextMarker = string.Empty;
                do
                {
                    var listObjectsRequest = new ListObjectsRequest(bucketName)
                    {
                        Marker = nextMarker,
                        MaxKeys = 200,
                        Prefix = prefix,
                    };
                    result = client.ListObjects(listObjectsRequest);
                    var list = result.ObjectSummaries.ToList().OrderByDescending(d => d.LastModified).Skip(maxSizeRollBackups).ToList();
                    foreach (var summary in list)
                    {
                        Console.WriteLine(summary.Key);
                        keys.Add(summary.Key);
                    }
                    nextMarker = result.NextMarker;
                } while (result.IsTruncated);
                Console.WriteLine("List objects of bucket:{0} succeeded ", bucketName);
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
            var client = new OssClient(endpoint, accessKeyId, accessKeySecret);
            try
            {
                var quietMode = false;
                var request = new DeleteObjectsRequest(bucketName, keys, quietMode);
                var result = client.DeleteObjects(request);
                if ((!quietMode) && (result.Keys != null))
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
