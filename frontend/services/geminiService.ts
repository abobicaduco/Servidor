
import { GoogleGenAI, Type, FunctionDeclaration } from "@google/genai";

const ai = new GoogleGenAI({ apiKey: process.env.API_KEY });

// Declaração de função para simular interação com BigQuery
const bigQueryTool: FunctionDeclaration = {
  name: 'validateBigQuerySQL',
  parameters: {
    type: Type.OBJECT,
    description: 'Valida se um SQL do BigQuery está correto e estima o processamento.',
    properties: {
      sql: { type: Type.STRING, description: 'O código SQL a ser validado.' },
      dataset: { type: Type.STRING, description: 'O dataset de destino.' }
    },
    required: ['sql']
  }
};

export const generateAutomationCode = async (prompt: string, type: 'PYTHON' | 'BIGQUERY'): Promise<{ code: string; explanation: string }> => {
  const model = 'gemini-3-pro-preview';
  const instruction = type === 'BIGQUERY' 
    ? "Você é um especialista em Google BigQuery. Gere o SQL padrão para o seguinte pedido."
    : "Você é um mestre em Python para engenharia de dados. Gere o script Python usando a biblioteca google-cloud-bigquery.";

  const response = await ai.models.generateContent({
    model: model,
    contents: `Pedido: ${prompt}`,
    config: {
      systemInstruction: instruction,
      responseMimeType: "application/json",
      responseSchema: {
        type: Type.OBJECT,
        properties: {
          code: { type: Type.STRING, description: "O código gerado (SQL ou Python)" },
          explanation: { type: Type.STRING, description: "Explicação técnica do que o código faz" }
        },
        required: ["code", "explanation"]
      }
    }
  });

  try {
    return JSON.parse(response.text || '{}');
  } catch (e) {
    return { code: "-- Erro ao gerar código", explanation: "Falha na resposta da IA" };
  }
};

export const translateToCron = async (naturalText: string): Promise<{ cron: string; explanation: string }> => {
  const response = await ai.models.generateContent({
    model: 'gemini-3-pro-preview',
    contents: `Traduza para cron: "${naturalText}"`,
    config: {
      responseMimeType: "application/json",
      responseSchema: {
        type: Type.OBJECT,
        properties: {
          cron: { type: Type.STRING },
          explanation: { type: Type.STRING }
        },
        required: ["cron", "explanation"]
      }
    }
  });
  return JSON.parse(response.text || '{}');
};
