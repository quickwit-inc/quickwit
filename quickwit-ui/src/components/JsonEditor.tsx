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
  // setting editor height based on lines height and count to stretch and fit its content
  const onMount = useCallback((editor) => {
    if (!resizeOnMount) {
      return;
    } 
    const editorElement = editor.getDomNode();

    if (!editorElement) {
      return;
    }

    // Magic number computed by hand to resize the editor to fit the content up to 800px.
    // In theory, we should be able to use https://github.com/microsoft/monaco-editor/issues/794#issuecomment-688959283
    // but sometimes we end up with a height > 7000px... and I don't know why.
    const height = Math.min(800, 18.81 * editor.getModel()?.getLineCount());
    editorElement.style.height = `${height}px`;
    editor.layout();
  }, [resizeOnMount]);

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
        wrappingIndent: 'indent',
      }}
      theme='quickwit-light'
    />
  )
}