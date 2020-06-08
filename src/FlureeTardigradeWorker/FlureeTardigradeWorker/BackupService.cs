using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting; 
using uplink.NET.Models;
using uplink.NET.Services;

namespace FlureeTardigradeWorker
{

    public interface IBackupService
    {
        Task<FileObject> GetFlureeSnapshot(string fileName);
        Task<int> UploadToCloud();
    }
    public  class BackupService:IBackupService
    {
        private readonly IHostEnvironment _hostEnvironment;
        private readonly IConfiguration _configuration;

        public BackupService(IHostEnvironment hostEnvironment, IConfiguration configuration)
        {
            _hostEnvironment = hostEnvironment;
            _configuration = configuration;
//            var builder = new ConfigurationBuilder()
//                .SetBasePath(hostEnvironment.ContentRootPath)
//                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
//                .AddJsonFile($"appsettings.{hostEnvironment.EnvironmentName}.json", optional: true)
//                .AddEnvironmentVariables(); 
//           Configuration = builder.Build();
        }
        public async Task<FileObject> GetFlureeSnapshot(string fileName)
        {
            var path = _hostEnvironment.ContentRootPath;
            var files = Directory.GetFiles($"{path}/Snapshots");
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
            uplink.NET.Models.Scope.SetTempDirectory(System.IO.Path.GetTempPath());
            var apiKey = _configuration["TardigradeCredential:ApiKey"];
            var secret = _configuration["TardigradeCredential:Secret"];
            var satelliteAddress = _configuration["TardigradeCredential:SatelliteAddress"]; 
            var scope = new uplink.NET.Models.Scope(apiKey, satelliteAddress, secret);
            var objectService = new ObjectService();
            // Listing buckets.
            var bucketService = new BucketService(scope);
            var listBucketOptions = new BucketListOptions();
            var buckets = await bucketService.ListBucketsAsync(listBucketOptions);
            foreach (BucketInfo b in buckets.Items)
            {
                Console.WriteLine(b.Name);
            } 
            var restrictedBucketService = new BucketService(scope); 

            var newBucketName = "flureebucket";
             
            var bucket = await restrictedBucketService.OpenBucketAsync(newBucketName);
            var fileObject = await GetFlureeSnapshot("1584526996862.avro");
            var uploadOperationRestricted = await objectService.UploadObjectAsync(bucket, fileObject.FileName, new UploadOptions(), fileObject.File, true);
            uploadOperationRestricted.UploadOperationProgressChanged += UploadOperationRestricted_UploadOperationProgressChanged;
            uploadOperationRestricted.UploadOperationEnded += UploadOperationRestricted_UploadOperationEnded;
            await uploadOperationRestricted.StartUploadAsync();


            // Download a file from a bucket.
            var listOptions = new ListOptions
            {
                Direction = ListDirection.STORJ_AFTER
            };
            var objects = await objectService.ListObjectsAsync(bucket, listOptions);

            foreach (var obj in objects.Items)
            {
                //work with the objects
                Console.WriteLine($"Found {obj.Path} {obj.Version}");


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
    
}
