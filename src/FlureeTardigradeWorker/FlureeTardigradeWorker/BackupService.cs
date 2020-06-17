using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json;
using uplink.NET.Models;
using uplink.NET.Services;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace FlureeTardigradeWorker
{

    public interface IBackupService
    {
        Task<FileObject> GetFlureeSnapshot(string fileName);
        Task<int> UploadToCloud();
        Task<int> UploadLargeFilesToCloud();
        Task<string> CreateSnapshot(string databaseName);
    }
    public  class BackupService:IBackupService
    {
        private readonly IHostEnvironment _hostEnvironment;
        private readonly IConfiguration _configuration;
        private readonly Access access;
        private readonly ObjectService objectService;

        public BackupService(IHostEnvironment hostEnvironment, IConfiguration configuration)
        {
            _hostEnvironment = hostEnvironment;
            _configuration = configuration;
            uplink.NET.Models.Access.SetTempDirectory(System.IO.Path.GetTempPath());
            var apiKey = _configuration["TardigradeCredential:ApiKey"];
            var secret = _configuration["TardigradeCredential:Secret"];
            var satelliteAddress = _configuration["TardigradeCredential:SatelliteAddress"];
             access = new uplink.NET.Models.Access(apiKey, satelliteAddress, secret);
             objectService = new ObjectService(access);

        }

        public async  Task<int> UploadLargeFilesToCloud()
        {
            #region "for large file upload; but the below code doesn't work porperly as it cann't append data at tardigrade server side;"
            var restrictedBucketService = new BucketService(access);

            var newBucketName = "flureebucket";
            var file = await GetFlureeSnapshot("file.mp4");
            var bucket = await restrictedBucketService.GetBucketAsync(newBucketName);
            using Stream source = File.OpenRead(file.FileName);
            int chunkSize = 4 * 1024 * 1024;
            long uploadSize = chunkSize;
            byte[] buffer = new byte[chunkSize];
            int bytesRead; 
            while ((bytesRead = source.Read(buffer, 0, buffer.Length)) > 0)
            {
                var uploadOperationRestricted = await objectService.UploadObjectAsync(bucket, file.FileName, new UploadOptions(), buffer, true);
                uploadOperationRestricted.UploadOperationProgressChanged += UploadOperationRestricted_UploadOperationProgressChanged;
                uploadOperationRestricted.UploadOperationEnded += UploadOperationRestricted_UploadOperationEnded;
                await uploadOperationRestricted.StartUploadAsync();

                var mb = ((float)uploadSize / (float)file.File.Length) * 100;
                Console.WriteLine($"{mb} % uploaded");
                uploadSize += chunkSize;
            }

            return 0;

            #endregion
        }

        public async Task<string> CreateSnapshot(string databaseName)
        {
            using var client = new HttpClient();
            client.BaseAddress = new Uri("http://localhost:8080");
            var payload = JsonConvert.SerializeObject(new CreateSnapshotModel {DbId = databaseName});
            var response = await client.PostAsync($"fdb/local/{databaseName}/snapshot", new StringContent(payload));
            if (response.StatusCode == HttpStatusCode.ServiceUnavailable)
                throw new Exception("Service Unavailable");
            response.EnsureSuccessStatusCode();
            var result = await response.Content.ReadAsStringAsync();
            return result;

        }
        public async Task<FileObject> GetFlureeSnapshot(string fileName)
        {
           // var result = await CreateSnapshot("local/firstdb");
            var path = _hostEnvironment.ContentRootPath;
            var files = Directory.GetFiles( $"{path}/Snapshots");
            var file = files.FirstOrDefault(x => x.Contains(fileName)) ?? throw new Exception($"File does not exist");
            var fileByte = await File.ReadAllBytesAsync(file);
            var fileInfo = new FileInfo(file);
            return new FileObject
            {
                File = fileByte,
                FileName = fileInfo.Name
            };
        }
        public async Task<int> UploadToCloud()
        {
           
            // Listing buckets.
            var bucketService = new BucketService(access);
          
            var buckets = await bucketService.ListBucketsAsync(new ListBucketsOptions());
            foreach (var b in buckets.Items)
            {
                Console.WriteLine(b.Name);
            } 
            var restrictedBucketService = new BucketService(access); 

            var newBucketName = "flureebucket";
             
            var bucket = await restrictedBucketService.GetBucketAsync(newBucketName);
            var fileObject = await GetFlureeSnapshot("1585578518736.avro");
            var uploadOperationRestricted = await objectService.UploadObjectAsync(bucket, fileObject.FileName, new UploadOptions(), fileObject.File, true);
            uploadOperationRestricted.UploadOperationProgressChanged += UploadOperationRestricted_UploadOperationProgressChanged;
            uploadOperationRestricted.UploadOperationEnded += UploadOperationRestricted_UploadOperationEnded;
            await uploadOperationRestricted.StartUploadAsync();


            // Download a file from a bucket.
            
            var objects = await objectService.ListObjectsAsync(bucket, new ListObjectsOptions());

            foreach (var obj in objects.Items)
            {
                //work with the objects
                Console.WriteLine($"Found {obj.Key} {obj.SystemMetaData.Created}");


                //await objectService.DeleteObjectAsync(bucket, obj.Path);

//                var downloadSvc = await objectService.DownloadObjectAsync(bucket, obj.Path, false);
//                downloadSvc.DownloadOperationProgressChanged += DownloadOperation_DownloadOperationProgressChanged;
//                downloadSvc.DownloadOperationEnded += DownloadOperation_DownloadOperationEnded;
//                await downloadSvc.StartDownloadAsync();
            }


            return 0;
        }

        private static void UploadOperationRestricted_UploadOperationEnded(UploadOperation uploadOperation)
        {
            if (uploadOperation.Completed)
            {
                Console.WriteLine("Upload Completed" + uploadOperation.ObjectName);
            }
        }

        private static void UploadOperationRestricted_UploadOperationProgressChanged(UploadOperation uploadOperation)
        {
            //Console.WriteLine($"{uploadOperation.ObjectName} {uploadOperation.PercentageCompleted}%");
            if (!string.IsNullOrEmpty(uploadOperation.ErrorMessage))
                Console.WriteLine(uploadOperation.ErrorMessage);
        }

        private static void DownloadOperation_DownloadOperationEnded(DownloadOperation downloadOperation)
        {
            if (downloadOperation.Completed)
            {
                Console.WriteLine("Download Completed" + downloadOperation.ObjectName);
            }
        }

        private static void DownloadOperation_DownloadOperationProgressChanged(DownloadOperation downloadOperation)
        {
            Console.WriteLine($"{downloadOperation.ObjectName} {downloadOperation.PercentageCompleted}%");
            if (!string.IsNullOrEmpty(downloadOperation.ErrorMessage))
                Console.WriteLine(downloadOperation.ErrorMessage);

            File.WriteAllBytes(downloadOperation.ObjectName, downloadOperation.DownloadedBytes);
        }

    }

    public class FileObject
    {
        public byte[] File { get; set; }
        public string FileName { get; set; }
    }

    public class CreateSnapshotModel
    {
        [JsonProperty("db/id")]
        public string DbId { get; set; }

    }

}
