using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace FlureeTardigradeWorker
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IBackupService _backupService;

        public Worker(ILogger<Worker> logger, IBackupService backupService)
        {
            _logger = logger;
            _backupService = backupService;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
           
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                 var result = await _backupService.UploadToCloud();
                _logger.LogInformation("Worker Completed at: {time} With result {res}", DateTimeOffset.Now, result);
                await Task.Delay(5000, stoppingToken);
            }
        }
    }
}
