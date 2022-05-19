// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

import Editor from "@monaco-editor/react";
import { useCallback } from "react";
import { EDITOR_THEME } from "../utils/theme";

export function JsonEditor({content, resizeOnMount}: {content: unknown, resizeOnMount: boolean}) {
  // Setting editor height based on lines height and count to stretch and fit its content.
  const onMount = useCallback((editor) => {
    if (!resizeOnMount) {
      return;
    } 
    const editorElement = editor.getDomNode();

    if (!editorElement) {
      return;
    }

    // Weirdly enough, we have to wait a few ms to get the right height
    // from `editor.getContentHeight()`. If not, we sometimes end up with
    // a height > 7000px... and I don't know why.
    setTimeout(() => {
      const height = Math.min(800, editor.getContentHeight());
      editorElement.style.height = `${height}px`;
      editor.layout();
    }, 10);
    
  }, [resizeOnMount]);

  /* eslint-disable  @typescript-eslint/no-explicit-any */
  function beforeMount(monaco: any) {
    monaco.editor.defineTheme('quickwit-light', EDITOR_THEME);
  }

  return (
    <Editor
      language='json'
      value={JSON.stringify(content, null, 2)}
      beforeMount={beforeMount}
      onMount={onMount}
      options={{
        readOnly: true,
        fontFamily: 'monospace',
        overviewRulerBorder: false,
        overviewRulerLanes: 0,
        minimap: {
          enabled: false,
        },
        scrollbar: {
          alwaysConsumeMouseWheel: false,
        },
        renderLineHighlight: "gutter",
        fontSize: 12,
        fixedOverflowWidgets: true,
        scrollBeyondLastLine: false,
        automaticLayout: true,
        wordWrap: 'on',
        wrappingIndent: 'advanced',
      }}
      theme='quickwit-light'
    />
  )
}