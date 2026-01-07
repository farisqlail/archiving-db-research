import { Controller, Post, Body } from '@nestjs/common';
import { AuthService } from './auth.service';

@Controller()
export class AuthController {
  constructor(private readonly authService: AuthService) {}

  @Post('login')
  async login(
    @Body('username') username: string,
    @Body('sandinaga') sandinaga: string,
  ) {
    return this.authService.login(username, sandinaga);
  }

  @Post('logout')
  async logout() {
    return this.authService.logout();
  }
}
