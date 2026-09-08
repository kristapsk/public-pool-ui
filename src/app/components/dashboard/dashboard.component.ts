import { AfterViewInit, Component, ViewChild } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Table } from 'primeng/table';
import { catchError, combineLatest, forkJoin, map, Observable, of, shareReplay, startWith, switchMap } from 'rxjs';

import { HashSuffixPipe } from '../../pipes/hash-suffix.pipe';
import { AppService } from '../../services/app.service';
import { ClientService } from '../../services/client.service';
import { WorkerService } from '../../services/worker.service';
import { AverageTimeToBlockPipe } from 'src/app/pipes/average-time-to-block.pipe';

const HASHES_PER_DIFFICULTY = 4294967296;

export interface GroupAverages {
  hourAvg: number;
  dayAvg: number;
  // ISO time of the group's oldest retained share (null = none recorded;
  // absent = backend predates the field). Durable across reconnects.
  oldestShareAt?: string | null;
}

export interface FullDayHistory {
  address: boolean;
  byName: Record<string, boolean>;
}



@Component({
  selector: 'app-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.scss']
})
export class DashboardComponent implements AfterViewInit {

  public address: string;

  public clientInfo$: Observable<any>;
  public clientInfoByPayoutMode$: Observable<{ pplns: any; solo: any; }>;
  public groupAverages$: Observable<Record<string, GroupAverages>>;
  public workerCount$: Observable<number>;
  public fullDayHistory$: Observable<FullDayHistory>;
  public chartData$: Observable<any>;

  public chartOptions: any;

  public networkInfo$: Observable<any>;
  private networkInfo:any;

  @ViewChild('dataTable') dataTable!: Table;

  public expandedRows$: Observable<any>;



  constructor(
    private clientService: ClientService,
    private workerService: WorkerService,
    private route: ActivatedRoute,
    private appService: AppService
  ) {

    this.networkInfo$ = this.appService.getNetworkInfo().pipe(
      shareReplay({ refCount: true, bufferSize: 1 })
    );

    this.address = this.route.snapshot.params['address'];
    this.clientInfo$ = this.clientService.getClientInfo(this.address).pipe(
      shareReplay({ refCount: true, bufferSize: 1 })
    );
    this.clientInfoByPayoutMode$ = forkJoin({
      pplns: this.clientService.getClientInfo(this.address, 'pplns'),
      solo: this.clientService.getClientInfo(this.address, 'solo')
    }).pipe(
      shareReplay({ refCount: true, bufferSize: 1 })
    );

    this.expandedRows$ = this.clientInfo$.pipe(map((info: any) => {

      return info.workers.reduce((pre: any, cur: any) => { pre[cur.name] = true; return pre; }, {});

    }));

    // Trailing averages come from the per-group accounting endpoint, which sums
    // credited work across sessions (address + worker name), so they survive
    // reconnects — unlike the per-session Hashrate column.
    this.groupAverages$ = this.clientInfo$.pipe(
      switchMap((info: any) => {
        const names: string[] = [...new Set<string>((info.workers ?? []).map((worker: any) => worker.name))];
        if (names.length === 0) {
          return of({} as Record<string, GroupAverages>);
        }
        return forkJoin(
          names.map(name => this.workerService.getGroupWorkerInfo(this.address, name).pipe(
            map((groupInfo: any) => ({
              name,
              averages: {
                hourAvg: Number(groupInfo?.accounting?.hashRateLastHour ?? 0),
                dayAvg: this.dayAverage(groupInfo?.accounting),
                oldestShareAt: groupInfo?.accounting?.oldestShareAt,
              } as GroupAverages | null,
            })),
            catchError(() => of({ name, averages: null as GroupAverages | null }))
          ))
        ).pipe(
          map(entries => entries.reduce((pre: Record<string, GroupAverages>, cur) => {
            if (cur.averages != null) {
              pre[cur.name] = cur.averages;
            }
            return pre;
          }, {}))
        );
      }),
      shareReplay({ refCount: true, bufferSize: 1 })
    );

    // Derived once per emission: the table reads these flags several times
    // per group row on every change-detection cycle, so a template-bound
    // method re-filtering the session list each call is wasted work
    // (review feedback). groupAverages$ starts with {} so the address tile
    // doesn't wait on the per-group fetches; group rows only render their
    // flag once their averages arrive anyway.
    this.fullDayHistory$ = combineLatest([
      this.clientInfo$,
      this.groupAverages$.pipe(startWith({} as Record<string, GroupAverages>)),
    ]).pipe(
      map(([info, groups]) => this.deriveFullDayHistory(info, groups)),
      shareReplay({ refCount: true, bufferSize: 1 })
    );

    // Same reasoning as above: the header binding would otherwise rebuild the
    // name Set on every change-detection pass (review feedback).
    this.workerCount$ = this.clientInfo$.pipe(
      map((info: any) => this.countWorkers(info.workers)),
      shareReplay({ refCount: true, bufferSize: 1 })
    );

    const documentStyle = getComputedStyle(document.documentElement);
    const textColor = documentStyle.getPropertyValue('--text-color');
    const textColorSecondary = documentStyle.getPropertyValue('--text-color-secondary');
    const surfaceBorder = documentStyle.getPropertyValue('--surface-border');
    const primaryColor = documentStyle.getPropertyValue('--primary-color');
    const soloColor = documentStyle.getPropertyValue('--yellow-600') || '#d97706';


    this.chartData$ = combineLatest([
      this.clientService.getClientInfoChartByPayoutMode(this.address, 'all'),
      this.networkInfo$
    ]).pipe(
      map(([chartData, networkInfo]) => {

        this.networkInfo = networkInfo;
        const datasets = this.toPayoutModeDatasets(chartData, {
          pplns: {
            label: 'PPLNS 10 Minute',
            borderColor: primaryColor,
            backgroundColor: (context: any) => this.getChartGradient(context, primaryColor)
          },
          solo: {
            label: 'Solo 10 Minute',
            borderColor: soloColor,
            backgroundColor: (context: any) => this.getChartGradient(context, soloColor)
          }
        });

        return {
          labels: chartData.map((d: any) => d.label),
          datasets
        }
      })
    );



    this.chartOptions = {
      maintainAspectRatio: false,
      plugins: {
        legend: {
          labels: {
            color: textColor
          }
        },
        tooltip: {
          callbacks: {
            label: (context: any) => this.getTooltipLabel(context),
            afterLabel: (context: any) => this.getTooltipDetails(context)
          }
        }
      },
      scales: {
        x: {
          type: 'time',
          time: {
            unit: 'hour', // Set the unit to 'minute'
          },
          ticks: {
            color: textColorSecondary
          },
          grid: {
            color: surfaceBorder,
            drawBorder: false,
            display: true
          }
        },
        yPplns: {
          position: 'left',
          title: {
            display: true,
            text: 'PPLNS',
            color: primaryColor
          },
          ticks: {
            color: primaryColor,
            callback: (value: number) => {
              return HashSuffixPipe.transform(value);
            }
          },
          grid: {
            color: surfaceBorder,
            drawBorder: false
          },
          beginAtZero: true
        },
        ySolo: {
          position: 'right',
          title: {
            display: true,
            text: 'Solo',
            color: soloColor
          },
          ticks: {
            color: soloColor,
            callback: (value: number) => {
              return HashSuffixPipe.transform(value);
            }
          },
          grid: {
            color: surfaceBorder,
            drawBorder: false,
            drawOnChartArea: false
          },
          beginAtZero: true
        }
      }
    };

  }



  ngAfterViewInit() {

  }

  // The accounting block reports hashRateLastHour but no day-window rate;
  // derive it from the credited-difficulty day sum. Fixed-window divisor:
  // a worker with less than a day of history reads low rather than being
  // extrapolated high (matches the backend's hashRateLastHour semantics).
  public dayAverage(accounting: any): number {
    return (Number(accounting?.creditedDifficultyLastDay ?? 0) * HASHES_PER_DIFFICULTY) / 86400;
  }

  public readonly partialHistoryHint =
    'Less than 24 h of observed history — the 24h average reads low until a full day accrues.';

  // Preferred age signal: the accounting block's oldestShareAt — the oldest
  // retained share for the address / worker group. It survives reconnects,
  // so a proxy restart or watchdog reconnect no longer flags a mature worker
  // as partial for up to a day. Sessions remain the fallback for a backend
  // that predates the field (absent ≠ null: null means genuinely no shares).
  private deriveFullDayHistory(info: any, groups: Record<string, GroupAverages>): FullDayHistory {
    const cutoff = Date.now() - 24 * 60 * 60 * 1000;
    const workers: any[] = info?.workers ?? [];
    let addressEarliest = Number.POSITIVE_INFINITY;
    const earliestByName: Record<string, number> = {};
    for (const worker of workers) {
      const started = new Date(worker.startTime).getTime();
      if (!Number.isFinite(started)) {
        continue;
      }
      addressEarliest = Math.min(addressEarliest, started);
      const prior = earliestByName[worker.name];
      earliestByName[worker.name] = prior == null ? started : Math.min(prior, started);
    }
    const fullDay = (oldestShareAt: string | null | undefined, sessionEarliest: number): boolean => {
      if (oldestShareAt === undefined) {
        return sessionEarliest <= cutoff;
      }
      if (oldestShareAt === null) {
        return false;
      }
      const oldest = new Date(oldestShareAt).getTime();
      return Number.isFinite(oldest) ? oldest <= cutoff : sessionEarliest <= cutoff;
    };
    const byName: Record<string, boolean> = {};
    for (const [name, earliest] of Object.entries(earliestByName)) {
      byName[name] = fullDay(groups[name]?.oldestShareAt, earliest);
    }
    return {
      address: fullDay(info?.accounting?.oldestShareAt, addressEarliest),
      byName,
    };
  }

  public getSessionCount(name: string, workers: any[]) {
    const workersByName = workers.filter(w => w.name == name);
    return workersByName.length;
  }

  /**
   * Distinct worker names — what the table calls a worker (rows group by
   * name; sessions are the expansion). The list now includes sessions from
   * the trailing day's share history, so counting raw entries would report
   * one worker's reconnects as several workers.
   */
  private countWorkers(workers: any[] | null | undefined): number {
    return new Set((workers ?? []).map(w => w.name)).size;
  }

  public getTotalHashRate(name: string, workers: any[]) {
    const workersByName = workers.filter(w => w.name == name);
    const sum = workersByName.reduce((pre, cur, idx, arr) => {
      return pre += Math.floor(cur.hashRate);
    }, 0);
    return Math.floor(sum);
  }

  public getBestDifficulty(name: string, workers: any[]) {
    const workersByName = workers.filter(w => w.name == name);
    const best = workersByName.reduce((pre, cur, idx, arr) => {
      if (cur.bestDifficulty > pre) {
        return cur.bestDifficulty;
      }
      return pre;
    }, 0);

    return best;
  }

  public getTotalUptime(name: string, workers: any[]) {
    const now = new Date().getTime();
    const workersByName = workers.filter(w => w.name == name);
    const sum = workersByName.reduce((pre, cur, idx, arr) => {
      return pre += now - new Date(cur.startTime).getTime();
    }, 0);
    return new Date(now - sum);
  }

  private toChartPoint(point: any) {
    return {
      y: Number(point.data),
      x: point.label,
      creditedWork: point.shares,
      payoutMode: point.payoutMode
    };
  }

  private toPayoutModeDatasets(chartData: any[], modes: Record<string, { label: string; borderColor: string; backgroundColor: any; }>) {
    return Object.entries(modes)
      .map(([mode, config]) => {
        const rows = chartData.filter(point => point.payoutMode === mode);

        return {
          type: 'line',
          label: config.label,
          data: rows.map((d: any) => this.toChartPoint(d)),
          yAxisID: mode === 'solo' ? 'ySolo' : 'yPplns',
          fill: true,
          backgroundColor: config.backgroundColor,
          borderColor: config.borderColor,
          tension: .4,
          pointRadius: 0,
          pointHoverRadius: 4,
          borderWidth: 2
        };
      })
      .filter(dataset => dataset.data.length > 0);
  }

  public getWorkerPayoutModes(name: string, workers: any[]): string[] {
    const modes = workers
      .filter(worker => worker.name === name)
      .map(worker => worker.payoutMode)
      .filter(mode => mode != null);
    return [...new Set(modes)];
  }

  public formatPayoutMode(mode: string | null | undefined): string {
    return mode === 'pplns' ? 'PPLNS' : 'Solo';
  }

  public getPayoutModeClass(mode: string | null | undefined): string {
    return mode === 'pplns' ? 'mode-badge mode-badge-pplns' : 'mode-badge mode-badge-solo';
  }

  private getTooltipLabel(context: any) {
    return `${context.dataset.label}: ${HashSuffixPipe.transform(context.parsed.y)}`;
  }

  private getTooltipDetails(context: any) {
    const raw = context.raw || {};
    const lines = [];
    if (raw.creditedWork !== undefined) {
      lines.push(`Credited work: ${Number(raw.creditedWork).toLocaleString(undefined, { maximumFractionDigits: 2 })}`);
    }

    if (this.networkInfo?.difficulty && context.parsed.y > 0) {
      lines.push(`Average time to block: ${AverageTimeToBlockPipe.transform(context.parsed.y, this.networkInfo.difficulty)}`);
    }

    return lines;
  }

  private getChartGradient(context: any, color: string) {
    const chart = context.chart;
    const chartArea = chart.chartArea;

    if (chartArea == null) {
      return this.toRgba(color, 0.2);
    }

    const gradient = chart.ctx.createLinearGradient(0, chartArea.top, 0, chartArea.bottom);
    gradient.addColorStop(0, this.toRgba(color, 0.32));
    gradient.addColorStop(0.65, this.toRgba(color, 0.09));
    gradient.addColorStop(1, this.toRgba(color, 0));
    return gradient;
  }

  private toRgba(color: string, alpha: number): string {
    const trimmed = color.trim();
    const hex = trimmed.match(/^#([0-9a-f]{3}|[0-9a-f]{6})$/i);
    if (hex != null) {
      const value = hex[1].length === 3
        ? hex[1].split('').map(part => part + part).join('')
        : hex[1];
      const red = parseInt(value.slice(0, 2), 16);
      const green = parseInt(value.slice(2, 4), 16);
      const blue = parseInt(value.slice(4, 6), 16);
      return `rgba(${red}, ${green}, ${blue}, ${alpha})`;
    }

    const rgb = trimmed.match(/^rgba?\(([^)]+)\)$/i);
    if (rgb != null) {
      const parts = rgb[1].split(',').map(part => part.trim()).slice(0, 3);
      return `rgba(${parts.join(', ')}, ${alpha})`;
    }

    return trimmed || `rgba(99, 102, 241, ${alpha})`;
  }
}
