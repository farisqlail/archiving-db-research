import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { ScheduleModule } from '@nestjs/schedule';
import { DatabaseModule } from './modules/database/database.module';
import { ArchivingModule } from './modules/archiving/archiving.module';

@Module({
  imports: [ScheduleModule.forRoot(), DatabaseModule, ArchivingModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
