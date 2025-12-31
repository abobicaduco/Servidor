
import React from 'react';
import { AutomationScript, JobStatus } from '../types';
import { PythonIcon, CheckIcon, AlertIcon, ClockIcon, PlayIcon, StopIcon } from './Icons';

interface AutomationCardProps {
  script: AutomationScript;
  onRun: (id: string) => void;
  onStop: (id: string) => void;
}

const AutomationCard: React.FC<AutomationCardProps> = ({ script, onRun, onStop }) => {
  
  // Lógica de Próxima Execução (Espelho do Backend)
  const getNextExecution = () => {
    if (!script.active || script.cron_type === 'MANUAL') return 'Manual Trigger';
    
    const now = new Date();
    const currentHour = now.getHours();
    
    if (script.cron_type === 'ALL') {
      // Se roda toda hora, a próxima é na virada da hora
      const nextH = (currentHour + 1) % 24;
      return `${nextH.toString().padStart(2, '0')}:00`;
    }

    if (script.cron_type === 'LIST' && script.cron_hours.length > 0) {
      // Procura a próxima hora na lista maior que a atual
      const upcoming = script.cron_hours.filter(h => h > currentHour).sort((a, b) => a - b);
      if (upcoming.length > 0) {
        return `${upcoming[0].toString().padStart(2, '0')}:00`;
      }
      return 'Amanhã'; // Só dia seguinte
    }

    return 'Manual';
  };

  const nextExecText = getNextExecution();

  // Estilização baseada no Status
  const getStatusStyle = () => {
    switch (script.status) {
      case JobStatus.RUNNING:
        return 'border-blue-500 shadow-[0_0_20px_rgba(59,130,246,0.2)] bg-[#0F1219]';
      case JobStatus.SUCCESS:
        return 'border-green-600/50 bg-[#0F1219]';
      case JobStatus.ERROR:
      case JobStatus.FAILED:
        return 'border-red-600/50 bg-[#1a0f0f]';
      case JobStatus.NO_DATA:
        return 'border-yellow-600/50 bg-[#1a160f]';
      case JobStatus.SCHEDULED:
        return 'border-slate-500/50 bg-[#0F1219]';
      default:
        return 'border-slate-800 bg-[#0F1219]';
    }
  };

  const formatTimestamp = (ts?: string) => {
    if (!ts) return '--:--:--';
    try {
      const date = new Date(ts);
      if (isNaN(date.getTime())) return '--:--:--';
      return date.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    } catch { return '--:--:--'; }
  };

  // Cálculo da barra de progresso
  const progressPercent = Math.min(100, Math.max(0, (script.daily_runs / (script.target_runs || 1)) * 100));

  return (
    <div className={`w-full h-[280px] rounded-2xl border-2 flex flex-col p-5 transition-all duration-300 relative group hover:scale-[1.02] ${getStatusStyle()}`}>
      
      {/* Indicador de Rodando (Ping) */}
      {script.status === JobStatus.RUNNING && (
        <span className="absolute top-4 right-4 flex h-3 w-3">
          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-75"></span>
          <span className="relative inline-flex rounded-full h-3 w-3 bg-blue-500"></span>
        </span>
      )}

      {/* Header */}
      <div className="flex justify-between items-start mb-3">
        <div className="flex items-center gap-2">
          <div className={`p-2 rounded-lg ${script.status === JobStatus.RUNNING ? 'bg-blue-500/20 text-blue-400' : 'bg-slate-800 text-slate-400'}`}>
            <PythonIcon />
          </div>
          <div className="flex flex-col">
             <span className="text-[10px] font-black text-slate-500 uppercase tracking-widest leading-none mb-1">
              {script.area_name}
            </span>
            <div className={`text-[10px] font-bold uppercase px-2 py-0.5 rounded w-fit ${
              script.status === JobStatus.RUNNING ? 'bg-blue-600 text-white' : 
              script.status === JobStatus.SUCCESS ? 'bg-green-900/40 text-green-400' :
              script.status === JobStatus.ERROR ? 'bg-red-900/40 text-red-400' :
              'bg-slate-800 text-slate-500'
            }`}>
              {script.status}
            </div>
          </div>
        </div>
      </div>

      {/* Nome do Script */}
      <h3 className="text-white font-bold text-base leading-tight mb-4 line-clamp-2 h-10" title={script.script_name}>
        {script.script_name.replace(/_/g, ' ')}
      </h3>

      {/* Informações de Execução */}
      <div className="flex-1 space-y-3">
        
        {/* Bloco Última Execução */}
        <div className="bg-black/20 rounded-lg p-2.5 border border-white/5 flex justify-between items-center">
          <div>
            <div className="text-[9px] font-bold text-slate-500 uppercase">Última Execução</div>
            <div className="text-xs font-mono text-slate-300">{formatTimestamp(script.last_execution?.timestamp)}</div>
          </div>
          <div className="text-right">
             <div className="text-[9px] font-bold text-slate-500 uppercase">Status</div>
             <div className="flex items-center justify-end gap-1">
                {script.last_execution?.status === 'SUCCESS' ? <CheckIcon /> : 
                 script.last_execution?.status?.includes('ERROR') ? <AlertIcon /> : null}
                <span className={`text-[10px] font-bold ${
                  script.last_execution?.status === 'SUCCESS' ? 'text-green-500' : 
                  script.last_execution?.status?.includes('ERROR') ? 'text-red-500' : 'text-slate-500'
                }`}>
                  {script.last_execution?.status || '---'}
                </span>
             </div>
          </div>
        </div>

        {/* Linha Próxima Execução */}
        <div className="flex items-center gap-2 text-slate-500 px-1">
          <ClockIcon />
          <span className="text-[10px] font-bold uppercase">
            Próxima: <span className={nextExecText === 'Amanhã' ? 'text-orange-400' : 'text-blue-400'}>{nextExecText}</span>
          </span>
        </div>

        {/* Barra de Meta Diária */}
        <div className="space-y-1 px-1">
          <div className="flex justify-between text-[9px] font-black text-slate-600 uppercase">
            <span>Runs Hoje</span>
            <span>{script.daily_runs} / {script.target_runs}</span>
          </div>
          <div className="w-full bg-slate-800 h-1.5 rounded-full overflow-hidden">
            <div 
              className={`h-full transition-all duration-1000 ${script.status === JobStatus.ERROR ? 'bg-red-500' : 'bg-blue-600'}`}
              style={{ width: `${progressPercent}%` }}
            />
          </div>
        </div>
      </div>

      {/* Botões de Ação */}
      <div className="grid grid-cols-2 gap-2 mt-3 pt-3 border-t border-white/5">
        <button 
          onClick={() => onRun(script.id)}
          disabled={script.status === JobStatus.RUNNING}
          className="bg-blue-600 hover:bg-blue-500 disabled:bg-slate-800 disabled:text-slate-600 disabled:cursor-not-allowed text-white py-2 rounded-lg text-[10px] font-black transition-all flex items-center justify-center gap-2 shadow-lg shadow-blue-900/20 active:scale-95"
        >
          <PlayIcon /> RUN
        </button>
        <button 
          onClick={() => onStop(script.id)}
          disabled={script.status !== JobStatus.RUNNING}
          className="bg-slate-800 hover:bg-red-900/40 disabled:opacity-50 disabled:hover:bg-slate-800 text-slate-400 hover:text-red-400 py-2 rounded-lg text-[10px] font-black transition-all border border-white/5 active:scale-95 flex items-center justify-center gap-2"
        >
          <StopIcon /> STOP
        </button>
      </div>
    </div>
  );
};

export default AutomationCard;
