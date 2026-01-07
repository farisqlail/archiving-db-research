import { ArgumentsHost, Catch, ExceptionFilter, HttpException, HttpStatus } from '@nestjs/common';
import type { Request, Response } from 'express';

@Catch()
export class GlobalHttpExceptionFilter implements ExceptionFilter {
  catch(exception: unknown, host: ArgumentsHost) {
    const ctx = host.switchToHttp();
    const response = ctx.getResponse<Response>();
    const request = ctx.getRequest<Request>();

    let status = HttpStatus.INTERNAL_SERVER_ERROR;
    let message = 'Internal server error';
    let error = 'InternalServerError';
    let details: any = undefined;

    if (exception instanceof HttpException) {
      status = exception.getStatus();
      const res = exception.getResponse();
      if (typeof res === 'string') {
        message = res;
        error = exception.name;
      } else if (typeof res === 'object' && res) {
        const obj = res as any;
        message = obj.message ?? message;
        error = obj.error ?? exception.name;
        details = obj.details ?? obj;
      }
    } else if (exception instanceof Error) {
      message = exception.message;
      error = exception.name || 'Error';
    }

    const payload: Record<string, any> = {
      statusCode: status,
      error,
      message,
      path: request?.url,
      timestamp: new Date().toISOString(),
    };
    if (details && status < 500) {
      payload.details = details;
    }

    response.status(status).json(payload);
  }
}
