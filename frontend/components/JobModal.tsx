
import React, { useState, useEffect } from 'react';
import { CronJob, JobStatus, JobType } from '../types';
import { SparklesIcon, BigQueryIcon, PythonIcon } from './Icons';
import { translateToCron, generateAutomationCode } from '../services/geminiService';

interface JobModalProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (job: Partial<CronJob>) => void;
  editingJob?: CronJob;
}

const JobModal: React.FC<JobModalProps> = ({ isOpen, onClose, onSave, editingJob }) => {
  const [formData, setFormData] = useState<Partial<CronJob>>({
    name: '',
    description: '',
    schedule: '* * * * *',
    type: JobType.PYTHON,
    code: '',
    tags: [],
    status: JobStatus.IDLE
  });

  const [aiPrompt, setAiPrompt] = useState('');
  const [isAiLoading, setIsAiLoading] = useState(false);
  const [activeTab, setActiveTab] = useState<'DETAILS' | 'CODE'>('DETAILS');

  useEffect(() => {
    if (editingJob) setFormData(editingJob);
    else setFormData({ name: '', description: '', schedule: '* * * * *', type: JobType.PYTHON, code: '', tags: [], status: JobStatus.IDLE });
  }, [editingJob, isOpen]);

  const handleAiGenerate = async () => {
    if (!aiPrompt.trim()) return;
    setIsAiLoading(true);
    try {
      // Tenta detectar se é um agendamento ou um código
      if (aiPrompt.toLowerCase().includes('cada') || aiPrompt.toLowerCase().includes('every')) {
        const res = await translateToCron(aiPrompt);
        setFormData(prev => ({ ...prev, schedule: res.cron }));
      } else {
        const res = await generateAutomationCode(aiPrompt, formData.type || JobType.PYTHON);
        setFormData(prev => ({ ...prev, code: res.code, description: res.explanation }));
        setActiveTab('CODE');
      }
    } catch (error) {
      console.error(error);
    } finally {
      setIsAiLoading(false);
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-950/90 backdrop-blur-md">
      <div className="bg-slate-900 border border-slate-700 w-full max-w-4xl h-[90vh] rounded-2xl shadow-2xl flex flex-col overflow-hidden">
        
        {/* Header com Abas */}
        <div className="px-6 py-4 border-b border-slate-800 flex justify-between items-center bg-slate-800/30">
          <div className="flex gap-4">
            <button 
              onClick={() => setActiveTab('DETAILS')}
              className={`px-4 py-2 rounded-lg text-sm font-semibold transition-all ${activeTab === 'DETAILS' ? 'bg-blue-600 text-white shadow-lg' : 'text-slate-400 hover:text-white'}`}
            >
              Configurações
            </button>
            <button 
              onClick={() => setActiveTab('CODE')}
              className={`px-4 py-2 rounded-lg text-sm font-semibold transition-all ${activeTab === 'CODE' ? 'bg-blue-600 text-white shadow-lg' : 'text-slate-400 hover:text-white'}`}
            >
              Código / Query
            </button>
          </div>
          <button onClick={onClose} className="text-slate-400 hover:text-white">&times;</button>
        </div>

        <div className="flex-1 overflow-y-auto p-8">
          
          {/* Assistente IA */}
          <div className="mb-8 p-4 bg-blue-500/5 border border-blue-500/20 rounded-xl">
            <label className="text-[10px] font-bold text-blue-400 uppercase tracking-widest mb-2 block">Assistente de Automação Gemini</label>
            <div className="flex gap-2">
              <input 
                type="text" 
                value={aiPrompt}
                onChange={e => setAiPrompt(e.target.value)}
                placeholder="Ex: 'Mova dados da tabela X para Y' ou 'Toda segunda as 8h'"
                className="flex-1 bg-slate-950 border border-slate-800 rounded-lg px-4 py-2 text-white focus:border-blue-500 outline-none"
              />
              <button 
                onClick={handleAiGenerate}
                disabled={isAiLoading}
                className="bg-blue-600 hover:bg-blue-500 disabled:opacity-50 text-white px-4 py-2 rounded-lg flex items-center gap-2 transition-all"
              >
                {isAiLoading ? 'Gerando...' : <><SparklesIcon /> Gerar</>}
              </button>
            </div>
          </div>

          {activeTab === 'DETAILS' ? (
            <div className="space-y-6">
              <div className="grid grid-cols-2 gap-6">
                <div className="space-y-2">
                  <label className="text-xs font-bold text-slate-500 uppercase">Nome do Job</label>
                  <input 
                    type="text" 
                    value={formData.name}
                    onChange={e => setFormData(prev => ({ ...prev, name: e.target.value }))}
                    className="w-full bg-slate-800/50 border border-slate-700 rounded-lg px-4 py-3 text-white"
                  />
                </div>
                <div className="space-y-2">
                  <label className="text-xs font-bold text-slate-500 uppercase">Tipo de Automação</label>
                  <div className="flex gap-2 p-1 bg-slate-950 border border-slate-800 rounded-xl">
                    <button 
                      onClick={() => setFormData(prev => ({ ...prev, type: JobType.PYTHON }))}
                      className={`flex-1 flex items-center justify-center gap-2 py-2 rounded-lg text-xs font-bold transition-all ${formData.type === JobType.PYTHON ? 'bg-slate-800 text-yellow-400 border border-slate-700' : 'text-slate-500'}`}
                    >
                      <PythonIcon /> Python
                    </button>
                    <button 
                      onClick={() => setFormData(prev => ({ ...prev, type: JobType.BIGQUERY }))}
                      className={`flex-1 flex items-center justify-center gap-2 py-2 rounded-lg text-xs font-bold transition-all ${formData.type === JobType.BIGQUERY ? 'bg-slate-800 text-blue-400 border border-slate-700' : 'text-slate-500'}`}
                    >
                      <BigQueryIcon /> BigQuery
                    </button>
                  </div>
                </div>
              </div>

              <div className="space-y-2">
                <label className="text-xs font-bold text-slate-500 uppercase">Agendamento (Cron)</label>
                <input 
                  type="text" 
                  value={formData.schedule}
                  onChange={e => setFormData(prev => ({ ...prev, schedule: e.target.value }))}
                  className="w-full bg-slate-800/50 border border-slate-700 rounded-lg px-4 py-3 text-blue-300 font-mono"
                />
              </div>

              <div className="space-y-2">
                <label className="text-xs font-bold text-slate-500 uppercase">Descrição Técnica</label>
                <textarea 
                  value={formData.description}
                  onChange={e => setFormData(prev => ({ ...prev, description: e.target.value }))}
                  className="w-full bg-slate-800/50 border border-slate-700 rounded-lg px-4 py-3 text-white h-32"
                />
              </div>
            </div>
          ) : (
            <div className="h-full flex flex-col space-y-2">
              <label className="text-xs font-bold text-slate-500 uppercase">Editor de Código ({formData.type})</label>
              <textarea 
                value={formData.code}
                onChange={e => setFormData(prev => ({ ...prev, code: e.target.value }))}
                className="flex-1 w-full bg-slate-950 border border-slate-800 rounded-xl p-6 text-blue-100 font-mono text-sm resize-none focus:border-blue-500 outline-none shadow-inner"
                spellCheck={false}
              />
            </div>
          )}
        </div>

        <div className="px-8 py-4 bg-slate-800/50 border-t border-slate-800 flex justify-end gap-4">
          <button onClick={onClose} className="px-6 py-2 text-slate-400 font-bold hover:text-white">Cancelar</button>
          <button 
            onClick={() => onSave(formData)}
            className="px-8 py-2 bg-blue-600 hover:bg-blue-500 text-white font-bold rounded-xl shadow-lg shadow-blue-600/20"
          >
            {editingJob ? 'Atualizar Automação' : 'Criar Automação'}
          </button>
        </div>
      </div>
    </div>
  );
};

export default JobModal;
