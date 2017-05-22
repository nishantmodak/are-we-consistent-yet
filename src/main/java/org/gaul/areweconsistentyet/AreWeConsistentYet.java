/*
 * Copyright 2014 Andrew Gaul <andrew@gaul.org>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gaul.areweconsistentyet;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import com.google.inject.Module;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.aws.s3.AWSS3ProviderMetadata;
import org.jclouds.aws.s3.blobstore.AWSS3BlobStoreContext;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.io.Payload;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.kohsuke.args4j.Option;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class AreWeConsistentYet {
	static String accessKey = "ENTER_AWS_ACCESS_KEY"; //System.getProperty("AWS_ACCESS_KEY");
	static String secretKey = "ENTER_AWS_SECRET_KEY"; //System.getProperty("AWS_SECRET_KEY");
	private final ByteSource payload1;
	private final ByteSource payload2;
	private final BlobStore blobStore;
	private final BlobStore blobStoreRead;
	private final String bucketName;
	private final int iterations;
	private final Random random = new Random();

	public AreWeConsistentYet(BlobStore blobStore,
	                          BlobStore blobStoreRead, String bucketName, int iterations,
	                          long objectSize) {
		this.blobStore = requireNonNull(blobStore);
		this.blobStoreRead = requireNonNull(blobStoreRead);
		this.bucketName = requireNonNull(bucketName);
		checkArgument(iterations > 0,
				"iterations must be greater than zero, was: " + iterations);
		this.iterations = iterations;
		checkArgument(objectSize > 0,
				"object size must be greater than zero, was: " + objectSize);
		payload1 = Utils.infiniteByteSource((byte) 1).slice(0, objectSize);
		payload2 = Utils.infiniteByteSource((byte) 2).slice(0, objectSize);
	}

	public static void main(String[] args) throws Exception {
		if (accessKey == null || secretKey == null) {
			System.err.println("no access key or secret defined");
			System.exit(1);
		}
		ContextBuilder builder = ContextBuilder.newBuilder(AWSS3ProviderMetadata.builder().build())
				.credentials(accessKey, secretKey)
				.modules(ImmutableList.<Module>of(new SLF4JLoggingModule()));
		BlobStoreContext context = builder.build(AWSS3BlobStoreContext.class);
		BlobStoreContext contextRead = builder.build(AWSS3BlobStoreContext.class);
		BlobStore blobStore = context.getBlobStore();
		BlobStore blobStoreRead = contextRead.getBlobStore();
		int iterations = 5;
		String containerName = "consistencytest134";
		long objectSize = 1L;

		AreWeConsistentYet test = new AreWeConsistentYet(blobStore, blobStoreRead, containerName, iterations, objectSize);
		System.out.println("eventual consistency count with " + iterations + " iterations: ");
		System.out.println("read after create: " + test.readAfterCreate());
		System.out.println("read after delete: " + test.readAfterDelete());
		System.out.println("read after overwrite: " + test.readAfterOverwrite());
		System.out.println("list after create: " + test.listAfterCreate());
		System.out.println("list after delete: " + test.listAfterDelete());
	}

	private static BlobStoreContext blobStoreContextFromProperties(Properties properties) {
		String provider = "aws-s3";
		String identity = "AWS_ACCESS_KEY";
		String credential = "AWS_SECRET_KEY";
		String endpoint = properties.getProperty(Constants.PROPERTY_ENDPOINT);
		if (provider == null || identity == null || credential == null) {
			System.err.println("Properties file must contain:\n" +
					Constants.PROPERTY_PROVIDER + "\n" +
					Constants.PROPERTY_IDENTITY + "\n" +
					Constants.PROPERTY_CREDENTIAL);
			System.exit(1);
		}

		ContextBuilder builder = ContextBuilder
				.newBuilder(provider)
				.credentials(identity, credential)
				.modules(ImmutableList.<Module>of(new SLF4JLoggingModule()))
				.overrides(properties);
		if (endpoint != null) {
			builder = builder.endpoint(endpoint);
		}
		return builder.build(BlobStoreContext.class);
	}

	public int readAfterCreate() throws IOException {
		int count = 0;
		for (int i = 0; i < iterations; ++i) {
			String blobName = makeBlobName();
			blobStore.putBlob(bucketName, makeBlob(blobName, payload1));
			Blob getBlob = blobStoreRead.getBlob(bucketName, blobName);
			if (getBlob == null) {
				++count;
			}
			blobStore.removeBlob(bucketName, blobName);
		}
		return count;
	}

	public int readAfterDelete() throws IOException {
		int count = 0;
		for (int i = 0; i < iterations; ++i) {
			String blobName = makeBlobName();
			blobStoreRead.putBlob(bucketName, makeBlob(blobName, payload1));
			blobStore.removeBlob(bucketName, blobName);
			Blob getBlob = blobStoreRead.getBlob(bucketName, blobName);
			if (getBlob != null) {
				++count;
				try (Payload payload = getBlob.getPayload();
				     InputStream is = payload.openStream()) {
					ByteStreams.copy(is, ByteStreams.nullOutputStream());
				}
			}
		}
		return count;
	}

	public int readAfterOverwrite() throws IOException {
		int count = 0;
		for (int i = 0; i < iterations; ++i) {
			String blobName = makeBlobName();
			blobStore.putBlob(bucketName, makeBlob(blobName, payload1));
			blobStore.putBlob(bucketName, makeBlob(blobName, payload2));
			Blob getBlob = blobStoreRead.getBlob(bucketName, blobName);
			if (getBlob == null) {
				++count;
				continue;
			}
			try (Payload payload = getBlob.getPayload();
			     InputStream is = payload.openStream()) {
				if (Arrays.equals(payload1.read(), ByteStreams.toByteArray(
						is))) {
					++count;
				}
			}
			blobStore.removeBlob(bucketName, blobName);
		}
		return count;
	}

	public int listAfterCreate() throws IOException {
		int count = 0;
		for (int i = 0; i < iterations; ++i) {
			String blobName = makeBlobName();
			blobStore.putBlob(bucketName, makeBlob(blobName, payload1));
			if (!listAllBlobs().contains(blobName)) {
				++count;
			}
			blobStore.removeBlob(bucketName, blobName);
		}
		return count;
	}

	public int listAfterDelete() throws IOException {
		int count = 0;
		for (int i = 0; i < iterations; ++i) {
			String blobName = makeBlobName();
			blobStoreRead.putBlob(bucketName, makeBlob(blobName, payload1));
			blobStore.removeBlob(bucketName, blobName);
			if (listAllBlobs().contains(blobName)) {
				++count;
			}
		}
		return count;
	}

	private String makeBlobName() {
		return "blob-name-" + random.nextInt();
	}

	private Blob makeBlob(String blobName, ByteSource payload)
			throws IOException {
		return blobStore.blobBuilder(blobName)
				.payload(payload)
				.contentLength(payload.size())
				.build();
	}

	private Set<String> listAllBlobs() {
		Set<String> blobNames = new HashSet<String>();
		ListContainerOptions options = new ListContainerOptions();
		while (true) {
			PageSet<? extends StorageMetadata> set = blobStoreRead.list(
					bucketName, options);
			for (StorageMetadata sm : set) {
				blobNames.add(sm.getName());
			}
			String marker = set.getNextMarker();
			if (marker == null) {
				break;
			}
			options = options.afterMarker(marker);
		}
		return blobNames;
	}

	static final class Options {
		@Option(name = "--container-name",
				usage = "container name for tests, will be created and removed",
				required = true)
		private String containerName;

		@Option(name = "--iterations", usage = "number of iterations (default: 1)")
		private int iterations = 1;

		@Option(name = "--location", usage = "container location")
		private String location;

		@Option(name = "--size", usage = "object size in bytes (default: 1)")
		private long objectSize = 1;

		@Option(name = "--properties", usage = "configuration file")
		private File propertiesFile;

		@Option(name = "--reader-endpoint", usage = "separate endpoint to read from")
		private String readerEndpoint;
	}
}
