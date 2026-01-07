import { Controller, Get } from '@nestjs/common';
import { LocationService } from './location.service';

@Controller('locations')
export class LocationController {
  constructor(private readonly locationService: LocationService) {}

  @Get()
  async findAll() {
    const data = await this.locationService.getAllLocations();
    return {
      status: 200,
      message: 'Success retrieving locations',
      data,
    };
  }
}
