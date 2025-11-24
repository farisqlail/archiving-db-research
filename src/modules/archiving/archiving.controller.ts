import { Controller, Get } from '@nestjs/common';
import { ArchivingService } from './archiving.service';

@Controller('archive')
export class ArchivingController {
  constructor(private readonly archivingService: ArchivingService) {}

  @Get('run')
  async run() {
    return this.archivingService.archive();
  }
}
