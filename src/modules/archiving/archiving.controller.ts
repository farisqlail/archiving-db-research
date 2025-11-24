import { Controller, Get, Query, BadRequestException } from '@nestjs/common';
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
}
