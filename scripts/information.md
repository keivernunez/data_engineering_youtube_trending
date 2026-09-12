Bronze Bucket Name - yt-data-pl-bronze
Silver Bucket Name - yt-data-pl-silver
Gold Bucket Name - yt-data-pl-gold

Scripts Bucket Name - yt-data-pl-scripts-dev

SNS ARN - arn:aws:sns:us-east-1:YOUR_ACCOUNT_ID:yt-data-pipeline-alerts-dev

Bronze Database Glue -  yt-pipeline-bronze-dev
Silver Database Glue -  yt-pipeline-silver-dev

--bronze_database yt-pipeline-bronze-dev
--bronze_table raw_statistics
--silver_bucket yt-data-pl-silver
--silver_database yt-pipeline-silver-dev
--silver_table clean_statistics

--gold_bucket   yt-data-pl-gold
--gold_database   yt-pipeline-gold-dev


