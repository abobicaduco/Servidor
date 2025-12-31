
import React from 'react';
import { CronJob, JobStatus, JobType } from '../types';
import { PlayIcon, PauseIcon, SettingsIcon, TrashIcon, BigQueryIcon, PythonIcon } from './Icons';

interface JobCardProps {
  job: CronJob;
  onToggle: (id: string) => void;
  onDelete: (id: string) => void;
  onEdit: (job: CronJob) => void;
}

const JobCard: React.FC<JobCardProps> = ({ job, onToggle, onDelete, onEdit }) => {
  const statusColors = {
    [JobStatus.RUNNING]: 'bg-green-500/10 text-green-400 border-green-500/20',
    [JobStatus.IDLE]: 'bg-blue-500/10 text-blue-400 border-blue-500/20',
    [JobStatus.FAILED]: 'bg-red-500/10 text-red-400 border-red-500/20',
    [JobStatus.DISABLED]: 'bg-slate-500/10 text-slate-400 border-slate-500/20',
  };

  return (
    <div className="bg-slate-800/50 border border-slate-700/50 rounded-xl p-5 hover:border-slate-600 transition-all group">
      <div className="flex justify-between items-start mb-4">
        <div className="flex items-start gap-3">
          <div className={`p-2 rounded-lg ${job.type === JobType.BIGQUERY ? 'bg-blue-500/20 text-blue-400' : 'bg-yellow-500/20 text-yellow-400'}`}>
            {job.type === JobType.BIGQUERY ? <BigQueryIcon /> : <PythonIcon />}
          </div>
          <div>
            <h3 className="text-lg font-semibold text-white group-hover:text-blue-400 transition-colors">{job.name}</h3>
            <p className="text-slate-400 text-sm mt-0.5 line-clamp-1">{job.description}</p>
          </div>
        </div>
        <span className={`px-2.5 py-1 rounded-full text-xs font-medium border ${statusColors[job.status]}`}>
          {job.status}
        </span>
      </div>

      <div className="bg-slate-900/50 rounded-lg p-3 mb-4">
        <div className="flex items-center gap-3">
          <div className="text-[10px] font-mono text-slate-500 uppercase tracking-wider">Schedule</div>
          <div className="text-sm font-mono text-blue-300 font-medium">{job.schedule}</div>
        </div>
        <div className="flex items-center gap-3 mt-2">
          <div className="text-[10px] font-mono text-slate-500 uppercase tracking-wider">Snippet</div>
          <div className="text-[11px] font-mono text-slate-400 truncate max-w-[180px]">
            {job.code.substring(0, 50)}...
          </div>
        </div>
      </div>

      <div className="flex items-center justify-between pt-4 border-t border-slate-700/50">
        <div className="flex gap-2">
          {job.tags.map(tag => (
            <span key={tag} className="text-[10px] bg-slate-700 text-slate-300 px-2 py-0.5 rounded uppercase font-bold">
              {tag}
            </span>
          ))}
        </div>
        <div className="flex items-center gap-2">
          <button onClick={() => onToggle(job.id)} className="p-2 text-slate-400 hover:text-white transition-colors">
            {job.status === JobStatus.DISABLED ? <PlayIcon /> : <PauseIcon />}
          </button>
          <button onClick={() => onEdit(job)} className="p-2 text-slate-400 hover:text-white transition-colors">
            <SettingsIcon />
          </button>
          <button onClick={() => onDelete(job.id)} className="p-2 text-slate-400 hover:text-red-400 transition-colors">
            <TrashIcon />
          </button>
        </div>
      </div>
    </div>
  );
};

export default JobCard;
