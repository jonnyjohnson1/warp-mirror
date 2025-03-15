{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  # Specify the Python version you want to use
  buildInputs = with pkgs; [
    python312
    just
    poetry
  ];

  # Environment setup
  shellHook = ''
    poetry install --no-root

    # Extract OPENAI_API_KEY from .env file and export it
    # if [ -f .env ]; then
    #   export OPENAI_API_KEY=$(grep -E '^OPENAI_API_KEY=' .env | cut -d '=' -f2)
    # fi
    
    # Set up environent with apis
    #  1. daemon -> pull casts from warpcast
    #  2. comfyui server -> processes the transcription workflow
    #  3. transcript-api server -> to use the transcription workflow
    #  4. stt and vid generation
    #  5. post video

    echo "OPENAI_API_KEY :: $OPENAI_API_KEY"
  '';
}