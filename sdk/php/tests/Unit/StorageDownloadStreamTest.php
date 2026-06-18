<?php

declare(strict_types=1);

use Fahara02\UdbLaravel\Services\StorageService;
use Fahara02\UdbLaravel\UdbProject;

/*
 * RB-PHP: StorageService streaming-download fallback (DownloadFile).
 *
 * UDB surface 264 -> 265 added the server-streaming StorageService.DownloadFile
 * RPC. StorageService::downloadFile() keeps PREFERRING the presigned
 * GetDownloadUrl URL (unchanged); the new downloadFileBytes() is the STREAMING
 * fallback that pulls the raw bytes through the broker and reassembles the
 * DownloadFileChunk byte-runs in order.
 *
 * Two layers of proof:
 *   1. A STATIC source guard (always runs, no ext-grpc/protobuf): downloadFileBytes
 *      issues EXACTLY one DownloadFile and never GetDownloadUrl/Get/List; the
 *      presigned downloadFile() path is unchanged.
 *   2. A runnable reassembly test (gated on ext-grpc + ext-protobuf, present in
 *      the CI image): inject a fake server-stream via the withDownloadStreamOpener
 *      seam, prove exactly one DownloadFile is opened and the chunk bytes
 *      concatenate into the original payload — no presigned URL fetched.
 */

/** Local single-method source extractor (self-contained; mirrors the slicer in
 *  NamingContractAliasesTest but without depending on its load order): from the
 *  needle to the next same-indent closing brace. */
function sliceStorageMethod(string $src, string $needle): string
{
    $start = strpos($src, $needle);
    if ($start === false) {
        return '';
    }
    $end = strpos($src, "\n    }", $start);

    return $end === false ? substr($src, $start) : substr($src, $start, $end - $start);
}

it('downloadFileBytes streams exactly one DownloadFile, no presigned/Get/List call', function () {
    $src = (string) file_get_contents(dirname(__DIR__, 2).'/src/Services/StorageService.php');
    $bytes = sliceStorageMethod($src, 'function downloadFileBytes');

    // Exactly one DownloadFile server-stream is opened (the default opener), and
    // the streaming path never touches the presigned URL or a Get/List probe.
    expect(substr_count($bytes, '->DownloadFile('))->toBe(1)
        ->and(substr_count($bytes, '->responses('))->toBe(1)
        ->and($bytes)->not->toContain('GetDownloadUrl')
        ->and($bytes)->not->toContain('->getDownloadUrl(')
        ->and(preg_match('/->(List|Get)[A-Z]\w*\(/', $bytes))->toBe(0);

    // The presigned default is unchanged: downloadFile() still delegates to
    // getDownloadUrl() and opens no stream.
    $download = sliceStorageMethod($src, 'function downloadFile(');
    expect(substr_count($download, '$this->getDownloadUrl('))->toBe(1)
        ->and($download)->not->toContain('->DownloadFile(')
        ->and($download)->not->toContain('->responses(');
});

it('StorageService exposes downloadFileBytes + the stream-opener seam', function () {
    expect(method_exists(StorageService::class, 'downloadFileBytes'))->toBeTrue('downloadFileBytes() missing')
        ->and(method_exists(StorageService::class, 'downloadFile'))->toBeTrue('downloadFile() presigned alias must remain')
        ->and(method_exists(StorageService::class, 'withDownloadStreamOpener'))->toBeTrue('withDownloadStreamOpener() seam missing');

    $ret = (new ReflectionMethod(StorageService::class, 'downloadFileBytes'))->getReturnType();
    expect($ret?->getName())->toBe('string');
});

it('downloadFileBytes opens exactly one DownloadFile stream and reassembles the chunks', function () {
    if (! extension_loaded('grpc') || ! extension_loaded('protobuf')) {
        test()->markTestSkipped('ext-grpc/ext-protobuf required to build the DownloadFileRequest + stub.');
    }

    $payload = 'hello-'.str_repeat('udb-bytes-', 5_000).'-tail';
    // Split the payload into ordered chunks; each fake chunk exposes getData()
    // exactly like the generated DownloadFileChunk.
    $parts = str_split($payload, 4096);
    $chunks = [];
    foreach ($parts as $p) {
        $chunks[] = new class($p)
        {
            public function __construct(private string $d)
            {
            }

            public function getData(): string
            {
                return $this->d;
            }
        };
    }

    // Fake server-streaming call: yields the chunks then an OK status. Records how
    // many times it was opened so we can assert EXACTLY one DownloadFile.
    $opened = 0;
    $fakeStream = new class($chunks)
    {
        public function __construct(private array $chunks)
        {
        }

        public function responses(): iterable
        {
            yield from $this->chunks;
        }

        public function getStatus(): object
        {
            return (object) ['code' => 0, 'details' => ''];
        }
    };

    $project = UdbProject::connect([
        'target' => '127.0.0.1:1',
        'tenantId' => 't_acme',
        'projectId' => 'default',
    ]);
    $storage = $project->storage();
    $sawRpc = null;
    $storage->withDownloadStreamOpener(function ($req, array $md) use (&$opened, &$sawRpc, $fakeStream) {
        $opened++;
        // The request must be the typed DownloadFileRequest carrying the file_id.
        $sawRpc = $req->getFileId();

        return $fakeStream;
    });

    $bytes = $storage->downloadFileBytes('file-123', 4096);

    expect($opened)->toBe(1)                 // exactly one DownloadFile stream opened
        ->and($sawRpc)->toBe('file-123')     // request carried the file id
        ->and($bytes)->toBe($payload);       // chunks reassembled in order, byte-exact
});
