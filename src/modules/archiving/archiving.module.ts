import { Module } from '@nestjs/common';
import { ArchivingService } from './archiving.service';
import { ArchivingController } from './archiving.controller';
import { DatabaseModule } from '../database/database.module';

@Module({
  imports: [DatabaseModule],
  controllers: [ArchivingController],
  providers: [ArchivingService],
})
export class ArchivingModule {}
