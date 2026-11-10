
export interface Note {
  id: string;
  [key: string]: unknown;
}

export interface NotePayload {
  id: string;
  [key: string]: unknown;
}

export const NOTES_TABLE = "public.notes";
