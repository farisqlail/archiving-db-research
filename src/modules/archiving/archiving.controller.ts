import {
  Controller,
  Get,
  Query,
  BadRequestException,
  Post,
  Body,
} from '@nestjs/common';
import { ArchivingService } from './archiving.service';

@Controller('archive')
export class ArchivingController {
  constructor(private readonly archivingService: ArchivingService) {}

  @Get('run')
  async run() {
    return this.archivingService.archive();
  }

  @Get('run-range')
  async runRange(
    @Query('from') from: string,
    @Query('to') to: string,
    @Query('column') column: string,
    @Query('tables') tables?: string,
  ) {
    if (!from || !to || !column) {
      throw new BadRequestException('from, to, column are required');
    }
    const t = tables
      ? tables
          .split(',')
          .map((s) => s.trim())
          .filter((s) => s.length > 0)
      : undefined;
    return this.archivingService.archiveRange({ from, to, column, tables: t });
  }

  @Post('backup')
  async backup(
    @Body('location_id') locationId: string,
    @Body('date_start') dateStart: string,
    @Body('date_end') dateEnd: string,
    @Body('column') column?: string,
    @Body('tables') tables?: string[],
  ) {
    if (!dateStart || !dateEnd) {
      throw new BadRequestException(
        'date_start and date_end are required',
      );
    }
    // If tables is empty or not provided, it means backup ALL tables
    const targetTables = tables && tables.length > 0 ? tables : [];

    const result = await this.archivingService.queueBackup({
      locationId,
      from: dateStart,
      to: dateEnd,
      column,
      tables: targetTables,
    });
    return {
      statusCode: 200,
      message: result.message,
      data: result,
    };
  }

  @Get('queue')
  async getQueue() {
    const data = await this.archivingService.getQueue();
    return {
      statusCode: 200,
      message: 'Success retrieving backup queue',
      data,
    };
  }

  @Post('queue/accept')
  async acceptQueue(@Body('id') id: number) {
    if (!id || Number.isNaN(Number(id))) {
      throw new BadRequestException('id is required');
    }
    const res = await this.archivingService.acceptQueueItem(Number(id));
    return {
      statusCode: res.status === 'COMPLETED' ? 200 : 400,
      message:
        res.status === 'COMPLETED'
          ? 'Queue item processed successfully'
          : res.error ?? 'Failed to process queue item',
      data: res,
    };
  }

  @Get('backup/detail')
  async backupDetail(
    @Query('location_id') locationId: string,
    @Query('date_start') dateStart: string,
    @Query('date_end') dateEnd: string,
    @Query('backup_at') backupAt: string,
  ) {
    if (!locationId || !dateStart || !dateEnd || !backupAt) {
      throw new BadRequestException(
        'location_id, date_start, date_end, and backup_at are required',
      );
    }
    const data = await this.archivingService.getBackupDetail({
      locationId,
      dateStart,
      dateEnd,
      backupAt,
    });
    return {
      statusCode: 200,
      message: 'Success retrieving backup detail',
      data,
    };
  }

  @Post('restore')
  async restore(
    @Body('location_id') locationId: string,
    @Body('date_start') dateStart: string,
    @Body('date_end') dateEnd: string,
    @Body('column') column?: string,
    @Body('tables') tables?: string[],
  ) {
    if (!locationId || !dateStart || !dateEnd) {
      throw new BadRequestException(
        'location_id, date_start, date_end are required',
      );
    }
    const result = await this.archivingService.restoreBackToMain({
      locationId,
      from: dateStart,
      to: dateEnd,
      column,
      tables,
    });
    return {
      statusCode: 200,
      message: 'Success restoring data from archive to main database',
      data: result,
    };
  }

  @Get('restore-data')
  async restoreData(
    @Query('location_id') locationId: string,
    @Query('date_start') dateStart: string,
    @Query('date_end') dateEnd: string,
    @Query('column') column?: string,
    @Query('tables') tables?: string,
  ) {
    if (!locationId || !dateStart || !dateEnd) {
      throw new BadRequestException(
        'location_id, date_start, date_end are required',
      );
    }
    const t = tables
      ? tables
          .split(',')
          .map((s) => s.trim())
          .filter((s) => s.length > 0)
      : undefined;

    return this.archivingService.getArchivedData({
      locationId,
      from: dateStart,
      to: dateEnd,
      column,
      tables: t,
    });
  }

  @Get('summary')
  async summary() {
    const data = await this.archivingService.getSummary();
    return {
      statusCode: 200,
      message: 'Success retrieving archive summary',
      data,
    };
  }

  @Get('restore-history')
  async restoreHistory() {
    const data = await this.archivingService.getRestoreHistory();
    return {
      statusCode: 200,
      message: 'Success retrieving restore history',
      data,
    };
  }

  @Get('backup')
  async backupHistory(
    @Query('date_start') dateStart?: string,
    @Query('date_end') dateEnd?: string,
  ) {
    const data = await this.archivingService.getBackupHistory({
      from: dateStart,
      to: dateEnd,
    });
    return {
      statusCode: 200,
      message: 'Success retrieving backup history',
      data,
    };
  }
}
